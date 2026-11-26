// Workflow test fixtures (M5): plain `effect/workflow` workflows calling the test agents.
import * as Workflows from "alchemy/Workflows";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Activity, Workflow } from "effect/workflow";
import * as Signal from "../../src/Signal.ts";
import { Echo, Flaky } from "./agents.ts";

const Echoed = Schema.Struct({ echoed: Schema.String });

/** Agent calls as durable steps: one, a parallel pair, a declared error, and a send. */
export const Greeting = Workflow.make("Greeting", {
  payload: { key: Schema.String },
  success: Schema.Struct({ a: Echoed, b: Echoed, c: Echoed, refused: Schema.String }),
  idempotencyKey: ({ key }) => key,
});
export const GreetingLive = Workflows.layer(
  Greeting,
  Effect.fn(function* ({ key }) {
    const echoes = yield* Echo.client;
    const a = yield* echoes(`wf-a-${key}`).EchoText({ text: "a" });
    const [b, c] = yield* Effect.all([echoes(`wf-b-${key}`).EchoText({ text: "b" }), echoes(`wf-c-${key}`).EchoText({ text: "c" })], {
      concurrency: "unbounded",
    });
    const refused = yield* (yield* Flaky.client)(`wf-x-${key}`)
      .Refuse()
      .pipe(
        Effect.as("not refused"),
        Effect.catchTag("Nope", (e) => Effect.succeed(`caught: ${e.reason}`)),
      );
    // A send: recorded once, not waited on.
    yield* echoes(`wf-d-${key}`).EchoKeyed({ id: key, text: "sent" }, { discard: true });
    return { a, b, c, refused };
  }, Effect.orDie),
);

export const Approve = Signal.make("approve", Schema.Struct({ approved: Schema.Boolean, by: Schema.String }));

/** Waits for an approval, with a durable timeout. */
export const Approval = Workflow.make("Approval", {
  payload: { key: Schema.String, timeoutMs: Schema.Number },
  success: Schema.Struct({ outcome: Schema.Literals(["approved", "rejected", "timeout"]), by: Schema.NullOr(Schema.String) }),
  idempotencyKey: ({ key }) => key,
});
export const ApprovalLive = Workflows.layer(
  Approval,
  Effect.fn(function* ({ timeoutMs }) {
    return yield* Signal.await(Approve, { timeout: `${timeoutMs} millis` }).pipe(
      Effect.map((a) => ({ outcome: a.approved ? ("approved" as const) : ("rejected" as const), by: a.by })),
      Effect.catchTag("SignalTimeout", () => Effect.succeed({ outcome: "timeout" as const, by: null })),
    );
  }),
);

/** Runs per execution in this isolate: the code changes between the first run and a replay. */
const shiftyRuns = new Map<string, number>();
/** Its first call changes target on replay (as if a deploy reordered the calls): the run must die. */
export const Shifty = Workflow.make("Shifty", { payload: { key: Schema.String }, idempotencyKey: ({ key }) => key });
export const ShiftyLive = Workflows.layer(
  Shifty,
  Effect.fn(function* ({ key }, executionId) {
    const n = (shiftyRuns.get(executionId) ?? 0) + 1;
    shiftyRuns.set(executionId, n);
    const echoes = yield* Echo.client;
    yield* echoes(n === 1 ? `shift-a-${key}` : `shift-b-${key}`).EchoText({ text: "x" });
  }, Effect.orDie),
);

/** An agent call inside an Activity: the run dies with an explanation. */
export const InActivity = Workflow.make("InActivity", { payload: { key: Schema.String }, idempotencyKey: ({ key }) => key });
export const InActivityLive = Workflows.layer(
  InActivity,
  Effect.fn(function* ({ key }) {
    yield* Activity.make({
      name: "nested",
      execute: Effect.flatMap(Echo.client, (echoes) => echoes(`nested-${key}`).EchoText({ text: "x" })).pipe(Effect.orDie, Effect.asVoid),
    });
  }),
);

export const workflows = [GreetingLive, ApprovalLive, ShiftyLive, InActivityLive];
