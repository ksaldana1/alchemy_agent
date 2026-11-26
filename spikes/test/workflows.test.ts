// Spike 0.4: the same Pipeline workflow on both engines (see src/wf/pipeline.ts and
// docs/spike/0.4-workflow-engines.md). Cloud by default (test_$USER); ALCHEMY_TEST_DEV=1 runs locally.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { Api } from "alchemy-agent";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Schedule from "effect/Schedule";
import type { Engine } from "../src/wf/pipeline.ts";
import Stack from "../src/wf/stack.ts";

process.env[Api.TOKEN_ENV] ??= "test-token";
const token = process.env[Api.TOKEN_ENV]!;

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

const run = crypto.randomUUID().slice(0, 8);
const timeout = { timeout: 180_000 };

type Status = { _tag: string; result?: unknown; error?: unknown; defect?: unknown };

const urlOf = (engine: Engine) => Effect.map(stack, (s) => (engine === "do" ? s.doUrl : s.nativeUrl)!);

const post = (url: string, body: unknown) =>
  Test.executeWhenReady(HttpClientRequest.post(url).pipe(HttpClientRequest.setBody(HttpBody.jsonUnsafe(body)))).pipe(
    Effect.flatMap((res) => (res.status === 204 ? Effect.succeed(null) : res.json)),
  );

const start = (engine: Engine, body: { key: string; ms: number; approvalTimeoutMs: number }) =>
  Effect.flatMap(urlOf(engine), (url) => post(`${url}/wf/start`, body)).pipe(
    Effect.map((j) => (j as { executionId: string }).executionId),
  );
const status = (engine: Engine, id: string) =>
  Effect.flatMap(urlOf(engine), (url) => HttpClient.get(`${url}/wf/${id}/status`)).pipe(
    Effect.flatMap((res) => res.json),
    Effect.map((j) => j as unknown as Status),
  );
const approve = (engine: Engine, id: string, ok: boolean) => Effect.flatMap(urlOf(engine), (url) => post(`${url}/wf/${id}/approve`, { ok }));
const finished = (engine: Engine, id: string) =>
  status(engine, id).pipe(
    Effect.repeat({ schedule: Schedule.spaced("500 millis"), until: (s) => s._tag !== "Running" && s._tag !== "NotStarted", times: 300 }),
  );

/** The task a run's n-th agent call created on `agent`, as the agent recorded it (over its HTTP API). */
const taskOf = (engine: Engine, agent: string, executionId: string, n: number) =>
  Effect.gen(function* () {
    const url = yield* urlOf(engine);
    const type = engine === "do" ? "JobDo" : "JobNative";
    const workflow = engine === "do" ? "PipelineDo" : "PipelineNative";
    const taskId = encodeURIComponent(`wf/${workflow}/${executionId}/c${n}`);
    const res = yield* HttpClient.execute(
      HttpClientRequest.get(`${url}/agents/${type}/${agent}/tasks/${taskId}`).pipe(HttpClientRequest.bearerToken(token)),
    );
    return res.status === 404 ? undefined : ((yield* res.json) as { status: string; attempts: number; causation: string });
  });

for (const engine of ["do", "native"] as const) {
  test(
    `${engine}: agent calls, a parallel pair, a declared error, a sleep, and an approval; each job runs once`,
    Effect.gen(function* () {
      const key = `full-${engine}-${run}`;
      const id = yield* start(engine, { key, ms: 300, approvalTimeoutMs: 0 });
      // Approve once the declared-error call (c3) has settled; the run then sleeps 2 s before it waits.
      yield* taskOf(engine, `x-${key}`, id, 3).pipe(
        Effect.repeat({ schedule: Schedule.spaced("500 millis"), until: (t) => t?.status === "failed", times: 120 }),
      );
      yield* Effect.sleep("4 seconds");
      expect((yield* status(engine, id))._tag).toBe("Running");
      yield* approve(engine, id, true);
      const done = yield* finished(engine, id);
      expect(done).toEqual({
        _tag: "Completed",
        result: {
          a: { label: `a-${key}`, runs: 1 },
          b: { label: `b-${key}`, runs: 1 },
          c: { label: `c-${key}`, runs: 1 },
          refused: "caught: never",
          approval: "approved",
        },
      });
      // W5: the run replayed from the top after every wake-up, yet each call is one task, run once.
      for (const [job, n] of [["a", 0], ["b", 1], ["c", 2]] as const) {
        expect(yield* taskOf(engine, `${job}-${key}`, id, n)).toMatchObject({
          status: "completed",
          attempts: 1,
          causation: `workflow/${engine === "do" ? "PipelineDo" : "PipelineNative"}/${id}`,
        });
      }
    }),
    timeout,
  );

  test(
    `${engine}: an approval sent before the run waits for it (twice) is kept and counts once`,
    Effect.gen(function* () {
      const key = `early-${engine}-${run}`;
      const id = yield* start(engine, { key, ms: 300, approvalTimeoutMs: 0 });
      yield* approve(engine, id, true);
      yield* approve(engine, id, false); // a second, different signal: must not change the outcome
      const done = yield* finished(engine, id);
      expect(done).toMatchObject({ _tag: "Completed", result: { approval: "approved" } });
    }),
    timeout,
  );

  test(
    `${engine}: without an approval, the durable timeout wins`,
    Effect.gen(function* () {
      const key = `timeout-${engine}-${run}`;
      const id = yield* start(engine, { key, ms: 100, approvalTimeoutMs: 3_000 });
      const done = yield* finished(engine, id);
      if (engine === "do") expect(done).toMatchObject({ _tag: "Completed", result: { approval: "timeout" } });
      // W9 on the native engine: a durable race isn't supported, so the run dies at the wait.
      else expect(done).toMatchObject({ _tag: "Died", defect: expect.stringMatching(/raceAll/) });
    }),
    timeout,
  );

  test(
    `${engine}: starting the same key twice is one run`,
    Effect.gen(function* () {
      const key = `dupe-${engine}-${run}`;
      const one = yield* start(engine, { key, ms: 100, approvalTimeoutMs: 0 });
      const two = yield* start(engine, { key, ms: 100, approvalTimeoutMs: 0 });
      expect(two).toBe(one);
      yield* approve(engine, one, true);
      expect(yield* finished(engine, one)).toMatchObject({ _tag: "Completed" });
      expect(yield* taskOf(engine, `a-${key}`, one, 0)).toMatchObject({ status: "completed", attempts: 1 });
    }),
    timeout,
  );
}
