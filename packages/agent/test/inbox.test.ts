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

/** Runs `effect` with the agents reachable over the deployed stack's HTTP API. */
const viaApi = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.flatMap(stack, ({ url }) =>
    effect.pipe(Effect.provide(Api.connect({ url: url!, token }, Echo, Flaky, Relay))),
  );

const timeout = { timeout: 60_000 };

test(
  "happy path: a call records the task, runs it, and returns its output",
  viaApi(
    Effect.gen(function* () {
      const { url } = yield* stack;
      yield* Test.getWhenReady(`${url}/shout/warmup`); // a fresh workers.dev URL can 404 briefly
      const echo = Echo.named(`a-${run}`);
      expect(yield* echo.echo({ text: "hi" }, { taskId: "t1" })).toEqual({ echoed: "HI" });
      expect(yield* echo.handle("echo", "t1").status).toMatchObject({
        taskId: "t1",
        task: "echo",
        status: "completed",
        attempts: 1,
        result: { echoed: "HI" },
      });
    }),
  ),
  timeout,
);

test(
  "send returns a handle right away; result waits (inside the DO, across a slow handler)",
  viaApi(
    Effect.gen(function* () {
      const handle = yield* Echo.named(`slow-${run}`).send.slow({ ms: 1500 });
      expect((yield* handle.status).status).not.toBe("completed");
      expect(yield* handle.result).toEqual({ slept: 1500 });
    }),
  ),
  timeout,
);

test(
  "AgentContext: type, name, task id, attempt",
  viaApi(
    Effect.gen(function* () {
      expect(yield* Echo.named(`ctx-${run}`).whoami(undefined, { taskId: "w1" })).toEqual({
        type: "Echo",
        name: `ctx-${run}`,
        taskId: "w1",
        attempt: 1,
      });
    }),
  ),
  timeout,
);

test(
  "dedupes by task id per agent; the same id on another agent is a different task",
  viaApi(
    Effect.gen(function* () {
      const a = Echo.named(`dedupe-a-${run}`);
      expect(yield* a.echo({ text: "one" }, { taskId: "k" })).toEqual({ echoed: "ONE" });
      // A settled task never runs again, and its outcome never changes.
      expect(yield* a.echo({ text: "two" }, { taskId: "k" })).toEqual({ echoed: "ONE" });
      expect((yield* a.handle("echo", "k").status).attempts).toBe(1);
      expect(yield* Echo.named(`dedupe-b-${run}`).echo({ text: "three" }, { taskId: "k" })).toEqual({
        echoed: "THREE",
      });
    }),
  ),
  timeout,
);

test(
  "runs tasks FIFO",
  viaApi(
    Effect.gen(function* () {
      const echo = Echo.named(`fifo-${run}`);
      const handles = [];
      for (const label of ["first", "second", "third"]) handles.push(yield* echo.send.order({ label }));
      const results = [];
      for (const h of handles) results.push(yield* h.result);
      expect(results.map((r) => r.label)).toEqual(["first", "second", "third"]);
      expect(results[0]!.n < results[1]!.n && results[1]!.n < results[2]!.n).toBe(true);
    }),
  ),
  timeout,
);

test(
  "a retryable failure is redelivered; waiting ignores non-final transitions",
  viaApi(
    Effect.gen(function* () {
      const flaky = Flaky.named(`retry-${run}`);
      expect(yield* flaky.flaky({ failTimes: 1 }, { taskId: "f" })).toEqual({ attempt: 2 });
      expect(yield* flaky.handle("flaky", "f").status).toMatchObject({
        status: "completed",
        attempts: 2,
        error: { _tag: "Flake", message: "flake #1" },
      });
    }),
  ),
  timeout,
);

test(
  "retries until attempts are exhausted, then fails with the last error",
  viaApi(
    Effect.gen(function* () {
      const failed = yield* Effect.flip(Flaky.named(`exhaust-${run}`).flaky({ failTimes: 10 }));
      expect(failed).toMatchObject({ _tag: "TaskFailed", attempts: 3, error: { _tag: "Flake", message: "flake #3" } });
    }),
  ),
  timeout,
);

test(
  "a PermanentError fails the task on the first attempt",
  viaApi(
    Effect.gen(function* () {
      const failed = yield* Effect.flip(Flaky.named(`perm-${run}`).permanent());
      expect(failed).toMatchObject({ attempts: 1, error: { _tag: "PermanentError", message: "nope" } });
    }),
  ),
  timeout,
);

test(
  "a defect is retried like any other failure",
  viaApi(
    Effect.gen(function* () {
      const failed = yield* Effect.flip(Flaky.named(`die-${run}`).die());
      expect(failed).toMatchObject({ attempts: 3, error: { _tag: "Defect", message: "boom" } });
    }),
  ),
  timeout,
);

test(
  "a running task whose execution is lost is redelivered; the caller's wait survives the reset",
  viaApi(
    Effect.gen(function* () {
      const flaky = Flaky.named(`crash-${run}`);
      expect(yield* flaky.crash({ times: 1 }, { taskId: "c" })).toEqual({ attempt: 2 });
      expect(yield* flaky.handle("crash", "c").status).toMatchObject({ attempts: 2, error: { _tag: "ExecutionLost" } });
    }),
  ),
  timeout,
);

test(
  "a task that keeps killing its execution ends up failed, not redelivered forever",
  viaApi(
    Effect.gen(function* () {
      const failed = yield* Effect.flip(Flaky.named(`crashloop-${run}`).crash({ times: 99 }));
      expect(failed).toMatchObject({ attempts: 3, error: { _tag: "ExecutionLost" } });
    }),
  ),
  timeout,
);

test(
  "an unknown task or invalid input is stored as failed, not thrown",
  viaApi(
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
      // A reference sees the stored failure when it waits.
      const failed = yield* Effect.flip(Echo.named(`bad-${run}`).handle("echo", "i").result);
      expect(failed).toMatchObject({ _tag: "TaskFailed", error: { _tag: "TaskInputError" } });
    }),
  ),
  timeout,
);

test(
  "waiting on an unknown task id fails with TaskNotFound",
  viaApi(
    Effect.gen(function* () {
      const error = yield* Effect.flip(Echo.named(`missing-${run}`).handle("echo", "never-sent").result);
      expect(error).toMatchObject({ _tag: "TaskNotFound", taskId: "never-sent" });
    }),
  ),
  timeout,
);

test(
  "an output schema stores the encoded form and the reference decodes it",
  viaApi(
    Effect.gen(function* () {
      const { url } = yield* stack;
      const out = yield* Echo.named(`stamp-${run}`).stamp({ epochMs: 0 }, { taskId: "s" });
      expect(out.at).toBeInstanceOf(Date);
      expect(out.at.getTime()).toBe(0);
      const raw = yield* HttpClient.execute(
        HttpClientRequest.get(`${url}/agents/Echo/stamp-${run}/tasks/s`).pipe(HttpClientRequest.bearerToken(token)),
      ).pipe(Effect.flatMap((res) => res.json));
      expect(raw).toMatchObject({ result: { at: "1970-01-01T00:00:00.000Z" } });
    }),
  ),
  timeout,
);

test(
  "an agent sends to another agent it declared in `uses`, with causation",
  viaApi(
    Effect.gen(function* () {
      expect(yield* Relay.named(`r-${run}`).forward({ text: "via" }, { taskId: "f" })).toEqual({ taskId: "f:echo" });
      const echoed = Echo.named(`from-r-${run}`).handle("echo", "f:echo");
      expect(yield* echoed.result).toEqual({ echoed: "VIA" });
      expect((yield* echoed.status).causation).toBe(`Relay/r-${run}/f`);
    }),
  ),
  timeout,
);

test(
  "a Worker's own route calls an agent through a reference (the API is mounted alongside)",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const res = yield* HttpClient.get(`${url}/shout/hello`);
    expect(yield* res.json).toEqual({ echoed: "HELLO" });
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
