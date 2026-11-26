// Spike 0.4: the same workflow on two engines. `make(engine)` builds a Worker that hosts a `Job`
// agent and the `Pipeline` workflow, and runs them on Alchemy's Durable Object engine ("do") or on
// its native Cloudflare Workflows engine ("native"). Only the engine Layer differs.
//
// Pipeline({ key, ms, approvalTimeout }):
//   1. a = jobs("a").Work(...)                         an agent call: a durable step that waits
//   2. [b, c] in parallel                              two waits at once
//   3. jobs("x").Refuse() caught with catchTag("Nope") a declared error crossing into the workflow
//   4. DurableClock.sleep("cooldown", 2 s)
//   5. approval: the `Approve` signal, raced against a durable timeout (or alone, if the timeout is 0)
//   6. { a, b, c, refused, approval }
import * as Cloudflare from "@alchemy.run/cloudflare";
import { Agent, Api } from "alchemy-agent";
import * as Testing from "alchemy-agent/testing";
import * as Workflows from "alchemy/Workflows";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";
import { DurableClock, DurableDeferred, Workflow } from "effect/workflow";

export type Engine = "do" | "native";

export class Nope extends Schema.TaggedError<Nope>()("Nope", { reason: Schema.String }) {}

export const Approve = DurableDeferred.make("approve", { success: Schema.Boolean });

const WorkResult = Schema.Struct({ label: Schema.String, runs: Schema.Number });

/** `main` is the module that default-exports the Worker (one per engine). */
export const make = <const E extends Engine>(engine: E, main: string) => {
  const suffix = engine === "do" ? "Do" : "Native";

  const Work = Rpc.make("Work", { payload: { label: Schema.String, ms: Schema.Number }, success: WorkResult });
  const Refuse = Rpc.make("Refuse", { error: Nope });
  const Job = Agent.make(`Job${suffix}`, [Work, Refuse]);

  const JobLive = Job.toLayer(
    Job.of({
      Work: Effect.fn("Job.Work")(function* ({ payload: { label, ms } }) {
        const runs = yield* Testing.count(`work:${label}`);
        yield* Effect.sleep(`${ms} millis`);
        return { label, runs };
      }),
      Refuse: () => Effect.fail(new Nope({ reason: "never" })),
    }),
  );

  const Pipeline = Workflow.make(`Pipeline${suffix}`, {
    payload: { key: Schema.String, ms: Schema.Number, approvalTimeoutMs: Schema.Number },
    success: Schema.Struct({
      a: WorkResult,
      b: WorkResult,
      c: WorkResult,
      refused: Schema.String,
      approval: Schema.Literals(["approved", "rejected", "timeout"]),
    }),
    idempotencyKey: ({ key }) => key,
  });

  const PipelineLive = Workflows.layer(
    Pipeline,
    Effect.fn(function* ({ key, ms, approvalTimeoutMs }) {
      const jobs = yield* Job.client;
      const a = yield* jobs(`a-${key}`).Work({ label: `a-${key}`, ms });
      const [b, c] = yield* Effect.all(
        [jobs(`b-${key}`).Work({ label: `b-${key}`, ms }), jobs(`c-${key}`).Work({ label: `c-${key}`, ms })],
        { concurrency: "unbounded" },
      );
      const refused = yield* jobs(`x-${key}`).Refuse().pipe(
        Effect.as("not refused"),
        Effect.catchTag("Nope", (e) => Effect.succeed(`caught: ${e.reason}`)),
      );
      yield* DurableClock.sleep({ name: "cooldown", duration: "2 seconds", inMemoryThreshold: 0 });
      const approved = DurableDeferred.await(Approve).pipe(Effect.map((ok) => (ok ? ("approved" as const) : ("rejected" as const))));
      // approvalTimeoutMs = 0: wait for the signal only (no durable race; the native engine has none).
      if (approvalTimeoutMs <= 0) return { a, b, c, refused, approval: yield* approved };
      const approval = yield* DurableDeferred.raceAll({
        name: "approval",
        success: Schema.Literals(["approved", "rejected", "timeout"]),
        error: Schema.Never,
        effects: [
          approved,
          DurableClock.sleep({ name: "approval-timeout", duration: `${approvalTimeoutMs} millis`, inMemoryThreshold: 0 }).pipe(
            Effect.as("timeout" as const),
          ),
        ],
      });
      return { a, b, c, refused, approval };
    }, Effect.orDie),
  );

  const EngineLive = engine === "do" ? Cloudflare.Workflows.layerDurableObjectEngine() : Cloudflare.Workflows.NativeEngine;

  // POST /wf/start {key, ms, approvalTimeoutMs} → {executionId}
  // GET  /wf/:id/status → Status;  POST /wf/:id/approve {ok} → 204;  /agents/... the agents' API.
  const Worker = Cloudflare.Worker(
    `Wf${suffix}`,
    { main },
    Effect.gen(function* () {
      const api = yield* Api.routes([Job]);
      const pipeline = yield* Workflows.RunWorkflow(Pipeline);
      return {
        fetch: Effect.gen(function* () {
          const fromApi = yield* api.handle;
          if (fromApi) return fromApi;
          const request = yield* HttpServerRequest;
          const path = new URL(request.url, "http://local").pathname;
          if (path === "/wf/start" && request.method === "POST") {
            const body = (yield* request.json) as { key: string; ms: number; approvalTimeoutMs: number };
            const run = yield* pipeline.start(body);
            return yield* HttpServerResponse.json({ executionId: run.executionId });
          }
          const m = path.match(/^\/wf\/([^/]+)\/(status|approve)$/);
          if (m) {
            const run = pipeline.get(decodeURIComponent(m[1]!));
            if (m[2] === "status") {
              const status = yield* run.status;
              // A defect is usually an Error, which JSON drops: send its message.
              return yield* HttpServerResponse.json(status._tag === "Died" ? { _tag: "Died", defect: String(status.defect) } : status);
            }
            const body = (yield* request.json) as { ok: boolean };
            yield* run.signal(Approve, body.ok);
            return HttpServerResponse.empty({ status: 204 });
          }
          return HttpServerResponse.text("Not found", { status: 404 });
        }).pipe(Effect.catchCause((cause) => Effect.succeed(HttpServerResponse.text(String(cause), { status: 500 })))),
      };
    }).pipe(Effect.provide(Layer.mergeAll(JobLive, PipelineLive).pipe(Layer.provideMerge(EngineLive)))),
  );

  return { Job, Pipeline, Worker };
};
