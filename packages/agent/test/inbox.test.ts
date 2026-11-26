// Inbox and drain-loop behavior, ported from mini-agent-runtime/tests/inbox.test.ts to the
// Durable Object model: an "incarnation" is one execution of the drain; "claimed" is `running`;
// releasing a dead incarnation's claims is the drain finding a `running` row when it starts.
//
// Local by default (`bun run test`); `bun run test:cloud` runs the same file on test_$USER.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Agent from "../src/Agent.ts";
import * as Api from "../src/Api.ts";
import { Echo, Flaky, Relay } from "./fixtures/agents.ts";
import Stack from "./fixtures/stack.ts";

process.env[Api.TOKEN_ENV] ??= "test-token";
const token = process.env[Api.TOKEN_ENV]!;

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

// Fresh agent names per run: deployed DO storage outlives a run.
const run = crypto.randomUUID().slice(0, 8);
const api = Effect.map(stack, ({ url }) => Api.client({ url: url!, token }));

/** The first request to a fresh workers.dev URL can 404 while the edge converges. */
const ready = Effect.gen(function* () {
  const { url } = yield* stack;
  yield* Test.getWhenReady(`${url}/agents/Echo/warmup/tasks/none`);
});

const timeout = { timeout: 60_000 };

test(
  "happy path: a task is recorded pending, runs, and completes with its output",
  Effect.gen(function* () {
    yield* ready;
    const echo = (yield* api).agent(Echo, `a-${run}`);
    const handle = yield* echo.send("echo", { text: "hi" }, { taskId: "t1" });
    expect(handle.taskId).toBe("t1");
    expect(yield* handle.result()).toEqual({ echoed: "HI" });
    expect(yield* handle.status).toMatchObject({
      taskId: "t1",
      task: "echo",
      status: "completed",
      attempts: 1,
      result: { echoed: "HI" },
    });
  }),
  timeout,
);

test(
  "AgentContext: type, name, task id, attempt",
  Effect.gen(function* () {
    const echo = (yield* api).agent(Echo, `ctx-${run}`);
    const handle = yield* echo.send("whoami", {}, { taskId: "w1" });
    expect(yield* handle.result()).toEqual({
      type: "Echo",
      name: `ctx-${run}`,
      taskId: "w1",
      attempt: 1,
    });
  }),
  timeout,
);

test(
  "dedupes by task id per agent; the same id on another agent is a different task",
  Effect.gen(function* () {
    const client = yield* api;
    const a = client.agent(Echo, `dedupe-a-${run}`);
    const b = client.agent(Echo, `dedupe-b-${run}`);
    const first = yield* a.send("echo", { text: "one" }, { taskId: "k" });
    yield* first.result();
    const dup = yield* a.send("echo", { text: "two" }, { taskId: "k" });
    const other = yield* b.send("echo", { text: "three" }, { taskId: "k" });
    // A settled task never runs again, and its outcome never changes.
    expect(yield* dup.result()).toEqual({ echoed: "ONE" });
    expect((yield* dup.status).attempts).toBe(1);
    expect(yield* other.result()).toEqual({ echoed: "THREE" });
  }),
  timeout,
);

test(
  "runs tasks FIFO",
  Effect.gen(function* () {
    const echo = (yield* api).agent(Echo, `fifo-${run}`);
    const handles = [];
    for (const label of ["first", "second", "third"]) {
      handles.push(yield* echo.send("order", { label }));
    }
    const results = [];
    for (const h of handles) results.push(yield* h.result());
    expect(results.map((r) => r.label)).toEqual(["first", "second", "third"]);
    expect(results[0]!.n < results[1]!.n && results[1]!.n < results[2]!.n).toBe(true);
  }),
  timeout,
);

test(
  "a retryable failure is redelivered; result() ignores non-final transitions",
  Effect.gen(function* () {
    const flaky = (yield* api).agent(Flaky, `retry-${run}`);
    const handle = yield* flaky.send("flaky", { failTimes: 1 });
    expect(yield* handle.result()).toEqual({ attempt: 2 });
    expect(yield* handle.status).toMatchObject({
      status: "completed",
      attempts: 2,
      error: { _tag: "Flake", message: "flake #1" },
    });
  }),
  timeout,
);

test(
  "retries until attempts are exhausted, then fails with the last error",
  Effect.gen(function* () {
    const flaky = (yield* api).agent(Flaky, `exhaust-${run}`);
    const handle = yield* flaky.send("flaky", { failTimes: 10 });
    const failed = yield* Effect.flip(handle.result());
    expect(failed).toMatchObject({
      _tag: "TaskFailed",
      attempts: 3,
      error: { _tag: "Flake", message: "flake #3" },
    });
  }),
  timeout,
);

test(
  "a PermanentError fails the task on the first attempt",
  Effect.gen(function* () {
    const flaky = (yield* api).agent(Flaky, `perm-${run}`);
    const failed = yield* Effect.flip((yield* flaky.send("permanent", {})).result());
    expect(failed).toMatchObject({ attempts: 1, error: { _tag: "PermanentError", message: "nope" } });
  }),
  timeout,
);

test(
  "a defect is retried like any other failure",
  Effect.gen(function* () {
    const flaky = (yield* api).agent(Flaky, `die-${run}`);
    const failed = yield* Effect.flip((yield* flaky.send("die", {})).result());
    expect(failed).toMatchObject({ attempts: 3, error: { _tag: "Defect", message: "boom" } });
  }),
  timeout,
);

test(
  "a running task whose execution is lost is redelivered as the next attempt",
  Effect.gen(function* () {
    const flaky = (yield* api).agent(Flaky, `crash-${run}`);
    const handle = yield* flaky.send("crash", { times: 1 });
    expect(yield* handle.result()).toEqual({ attempt: 2 });
    expect(yield* handle.status).toMatchObject({
      attempts: 2,
      error: { _tag: "ExecutionLost" },
    });
  }),
  timeout,
);

test(
  "a task that keeps killing its execution ends up failed, not redelivered forever",
  Effect.gen(function* () {
    const flaky = (yield* api).agent(Flaky, `crashloop-${run}`);
    const failed = yield* Effect.flip((yield* flaky.send("crash", { times: 99 })).result());
    expect(failed).toMatchObject({ attempts: 3, error: { _tag: "ExecutionLost" } });
  }),
  timeout,
);

test(
  "an unknown task or invalid input is stored as failed, not thrown",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const post = (body: unknown) =>
      HttpClient.execute(
        HttpClientRequest.post(`${url}/agents/Echo/bad-${run}/tasks`).pipe(
          HttpClientRequest.bearerToken(token),
          HttpClientRequest.setBody(HttpBody.jsonUnsafe(body)),
        ),
      ).pipe(Effect.flatMap((res) => Effect.map(res.json, (json) => ({ status: res.status, json }))));

    expect(yield* post({ task: "nope", input: {}, taskId: "u" })).toMatchObject({
      status: 202,
      json: { status: "failed", attempts: 0, error: { _tag: "UnknownTaskError" } },
    });
    expect(yield* post({ task: "echo", input: { text: 1 }, taskId: "i" })).toMatchObject({
      status: 202,
      json: { status: "failed", attempts: 0, error: { _tag: "TaskInputError" } },
    });
    // The typed client sees the stored failure through result().
    const echo = (yield* api).agent(Echo, `bad-${run}`);
    const failed = yield* Effect.flip(echo.handle("echo", "i").result());
    expect(failed).toMatchObject({ _tag: "TaskFailed", error: { _tag: "TaskInputError" } });
  }),
  timeout,
);

test(
  "result() for an unknown task id fails with TaskNotFound",
  Effect.gen(function* () {
    const echo = (yield* api).agent(Echo, `missing-${run}`);
    const error = yield* Effect.flip(echo.handle("echo", "never-sent").result());
    expect(error).toMatchObject({ _tag: "TaskNotFound", taskId: "never-sent" });
  }),
  timeout,
);

test(
  "an output schema stores the encoded form and the client decodes it",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const echo = (yield* api).agent(Echo, `stamp-${run}`);
    const handle = yield* echo.send("stamp", { epochMs: 0 }, { taskId: "s" });
    const out = yield* handle.result();
    expect(out.at).toBeInstanceOf(Date);
    expect(out.at.getTime()).toBe(0);
    const raw = yield* HttpClient.execute(
      HttpClientRequest.get(`${url}/agents/Echo/stamp-${run}/tasks/s`).pipe(
        HttpClientRequest.bearerToken(token),
      ),
    ).pipe(Effect.flatMap((res) => res.json));
    expect(raw).toMatchObject({ result: { at: "1970-01-01T00:00:00.000Z" } });
  }),
  timeout,
);

test(
  "an agent sends to another agent it declared in `uses`, with causation",
  Effect.gen(function* () {
    const client = yield* api;
    const relay = client.agent(Relay, `r-${run}`);
    const forwarded = yield* (yield* relay.send("forward", { text: "via" }, { taskId: "f" })).result();
    expect(forwarded).toEqual({ taskId: "f:echo" });
    const echoed = client.agent(Echo, `from-r-${run}`).handle("echo", "f:echo");
    expect(yield* echoed.result()).toEqual({ echoed: "VIA" });
    expect((yield* echoed.status).causation).toBe(`Relay/r-${run}/f`);
  }),
  timeout,
);

test(
  "the API rejects a missing or wrong bearer token",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const status = (auth?: string) =>
      HttpClient.execute(
        HttpClientRequest.get(`${url}/agents/Echo/a/tasks/x`).pipe(
          auth ? HttpClientRequest.setHeader("authorization", auth) : (r) => r,
        ),
      ).pipe(Effect.map((res) => res.status));
    expect(yield* status()).toBe(401);
    expect(yield* status("Bearer wrong")).toBe(401);
    expect(yield* status(`Bearer ${token}`)).toBe(404);
  }),
  timeout,
);

// Keep `Agent` referenced for readers jumping here from the type tests.
void Agent;
