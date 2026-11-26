// Workflows (M5): plain `effect/workflow` workflows on the Durable Object engine, hosted with
// `Platform.layer`, driven over the HTTP API. Agent calls inside a run are durable steps.
//
// Local by default (`bun run test`); `bun run test:cloud` runs the same file on test_$USER.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Schedule from "effect/Schedule";
import * as Api from "../src/Api.ts";
import Stack from "./fixtures/stack.ts";

process.env[Api.TOKEN_ENV] ??= "test-token";
const token = process.env[Api.TOKEN_ENV]!;

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

const run = crypto.randomUUID().slice(0, 8);
const timeout = { timeout: 120_000 };
const local = !!process.env.ALCHEMY_TEST_DEV;

type Status = { _tag: string; result?: any; error?: unknown; defect?: string };

const http = (request: HttpClientRequest.HttpClientRequest) =>
  Effect.flatMap(stack, ({ url }) =>
    Test.executeWhenReady(
      request.pipe(HttpClientRequest.prependUrl(url!), HttpClientRequest.bearerToken(token)),
    ).pipe(Effect.flatMap((res) => Effect.map(res.json, (body) => ({ status: res.status, body: body as any })))),
  );

/** Without waiting for the Worker to be ready (which retries 4xx answers). */
const httpOnce = (request: HttpClientRequest.HttpClientRequest) =>
  Effect.flatMap(stack, ({ url }) =>
    HttpClient.execute(request.pipe(HttpClientRequest.prependUrl(url!), HttpClientRequest.bearerToken(token))).pipe(
      Effect.map((res) => res.status),
    ),
  );

const start = (workflow: string, payload: unknown) =>
  http(HttpClientRequest.post(`/workflows/${workflow}/runs`).pipe(HttpClientRequest.setBody(HttpBody.jsonUnsafe({ payload })))).pipe(
    Effect.tap((r) => Effect.sync(() => expect(r.status).toBe(202))),
    Effect.map((r) => r.body.executionId as string),
  );
const status = (workflow: string, id: string) =>
  http(HttpClientRequest.get(`/workflows/${workflow}/runs/${id}`)).pipe(Effect.map((r) => r.body as Status));
const signal = (workflow: string, id: string, name: string, value: unknown) =>
  http(HttpClientRequest.post(`/workflows/${workflow}/runs/${id}/signals/${name}`).pipe(HttpClientRequest.setBody(HttpBody.jsonUnsafe({ value }))));
const finished = (workflow: string, id: string) =>
  status(workflow, id).pipe(
    Effect.repeat({ schedule: Schedule.spaced("250 millis"), until: (s) => s._tag !== "Running" && s._tag !== "NotStarted", times: 400 }),
  );
const task = (type: string, name: string, taskId: string) =>
  http(HttpClientRequest.get(`/agents/${type}/${name}/tasks/${encodeURIComponent(taskId)}`)).pipe(Effect.map((r) => r.body));

test(
  "agent calls in a workflow are durable steps: one, a parallel pair, a declared error, and a send",
  Effect.gen(function* () {
    const key = `g-${run}`;
    const id = yield* start("Greeting", { key });
    expect(yield* finished("Greeting", id)).toEqual({
      _tag: "Completed",
      result: { a: { echoed: "A" }, b: { echoed: "B" }, c: { echoed: "C" }, refused: "caught: never" },
    });
    // Each call is one task, run once, caused by the run. The send used its primaryKey.
    for (const [name, n] of [[`wf-a-${key}`, 0], [`wf-b-${key}`, 1], [`wf-c-${key}`, 2]] as const) {
      expect(yield* task("Echo", name, `wf/Greeting/${id}/c${n}`)).toMatchObject({
        status: "completed",
        attempts: 1,
        causation: `workflow/Greeting/${id}`,
      });
    }
    yield* task("Echo", `wf-d-${key}`, `EchoKeyed:${key}`).pipe(
      Effect.repeat({ schedule: Schedule.spaced("250 millis"), until: (t) => t.status === "completed", times: 40 }),
    );
  }),
  timeout,
);

test(
  "starting the same key twice is the same run",
  Effect.gen(function* () {
    const one = yield* start("Greeting", { key: `dupe-${run}` });
    expect(yield* start("Greeting", { key: `dupe-${run}` })).toBe(one);
    expect(yield* finished("Greeting", one)).toMatchObject({ _tag: "Completed" });
  }),
  timeout,
);

test(
  "a signal resumes a waiting run with a typed value; later signals are ignored",
  Effect.gen(function* () {
    const id = yield* start("Approval", { key: `ok-${run}`, timeoutMs: 120_000 });
    yield* Effect.sleep("1 second");
    expect((yield* status("Approval", id))._tag).toBe("Running");
    expect((yield* signal("Approval", id, "approve", { approved: true, by: "ada" })).status).toBe(202);
    yield* signal("Approval", id, "approve", { approved: false, by: "eve" });
    expect(yield* finished("Approval", id)).toEqual({ _tag: "Completed", result: { outcome: "approved", by: "ada" } });
  }),
  timeout,
);

test(
  "a signal sent before the run waits for it is kept",
  Effect.gen(function* () {
    const id = yield* start("Approval", { key: `early-${run}`, timeoutMs: 120_000 });
    yield* signal("Approval", id, "approve", { approved: false, by: "bob" });
    expect(yield* finished("Approval", id)).toEqual({ _tag: "Completed", result: { outcome: "rejected", by: "bob" } });
  }),
  timeout,
);

test(
  "without a signal the durable timeout wins, and a signal after it changes nothing",
  Effect.gen(function* () {
    const id = yield* start("Approval", { key: `late-${run}`, timeoutMs: 1_500 });
    expect(yield* finished("Approval", id)).toEqual({ _tag: "Completed", result: { outcome: "timeout", by: null } });
    yield* signal("Approval", id, "approve", { approved: true, by: "late" });
    yield* Effect.sleep("1 second");
    expect(yield* status("Approval", id)).toEqual({ _tag: "Completed", result: { outcome: "timeout", by: null } });
  }),
  timeout,
);

test(
  "an invalid payload is a 400, an unknown workflow a 404",
  Effect.gen(function* () {
    const bad = HttpClientRequest.post(`/workflows/Approval/runs`).pipe(HttpClientRequest.setBody(HttpBody.jsonUnsafe({ payload: { key: 1 } })));
    expect(yield* httpOnce(bad)).toBe(400);
    expect(yield* httpOnce(HttpClientRequest.get(`/workflows/Nope/runs/x`))).toBe(404);
  }),
  timeout,
);

test(
  "an agent call inside an Activity fails the run with an explanation",
  Effect.gen(function* () {
    const id = yield* start("InActivity", { key: `act-${run}` });
    const done = yield* finished("InActivity", id);
    expect(done._tag).toBe("Died");
    expect(done.defect).toMatch(/inside an Activity/);
  }),
  timeout,
);

// The guard needs the code to change between runs; only a local isolate (which replays in the
// same isolate) makes that deterministic.
test.skipIf(!local)(
  "a replay that makes a different call at the same position dies instead of reusing another call's answer",
  Effect.gen(function* () {
    const id = yield* start("Shifty", { key: `shift-${run}` });
    const done = yield* finished("Shifty", id);
    expect(done._tag).toBe("Died");
    expect(done.defect).toMatch(/must make the same calls in the same order/);
  }),
  timeout,
);
