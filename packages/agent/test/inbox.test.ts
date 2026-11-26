// Inbox and drain-loop behavior, ported from mini-agent-runtime/tests/inbox.test.ts to the
// Durable Object model: an "incarnation" is one execution of the drain; "claimed" is `running`;
// releasing a dead incarnation's claims is the drain finding a `running` row when it starts.
//
// Local by default (`bun run test`); `bun run test:cloud` runs the same file on test_$USER.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Agent from "../src/Agent.ts";
import * as Api from "../src/Api.ts";
import { Echo, Flaky, Nope, Relay } from "./fixtures/agents.ts";
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

/** Runs `effect` with every client going over the deployed stack's HTTP API. */
const viaApi = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.flatMap(stack, ({ url }) => effect.pipe(Effect.provide(Api.connect({ url: url!, token }))));

/** The raw stored record of a task, over HTTP. */
const record = (type: string, name: string, taskId: string) =>
  Effect.flatMap(stack, ({ url }) =>
    HttpClient.execute(
      HttpClientRequest.get(`${url}/agents/${type}/${name}/tasks/${encodeURIComponent(taskId)}`).pipe(HttpClientRequest.bearerToken(token)),
    ).pipe(Effect.flatMap((res) => res.json), Effect.map((j) => j as unknown as Agent.TaskRecord)),
  );

const timeout = { timeout: 60_000 };

/** A task's record once it has settled (it may not exist yet: sends are delivered asynchronously). */
const settledRecord = (type: string, name: string, taskId: string) =>
  record(type, name, taskId).pipe(
    Effect.repeat({
      schedule: Schedule.spaced("250 millis"),
      until: (r) => r.status === "completed" || r.status === "failed",
      times: 120,
    }),
  );

test(
  "happy path: a call records the task, runs it, and returns its output",
  viaApi(
    Effect.gen(function* () {
      const { url } = yield* stack;
      yield* Test.getWhenReady(`${url}/shout/warmup`); // a fresh workers.dev URL can 404 briefly
      const echo = (yield* Echo.client)(`a-${run}`);
      expect(yield* echo.EchoText({ text: "hi" })).toEqual({ echoed: "HI" });
      expect(yield* echo.EchoKeyed({ id: "t1", text: "hi" })).toEqual({ echoed: "HI" });
      expect(yield* record("Echo", `a-${run}`, "EchoKeyed:t1")).toMatchObject({
        taskId: "EchoKeyed:t1",
        task: "EchoKeyed",
        status: "completed",
        attempts: 1,
        result: { echoed: "HI" },
      });
    }),
  ),
  timeout,
);

test(
  "handlers receive the agent, task id, and attempt",
  viaApi(
    Effect.gen(function* () {
      const me = yield* (yield* Echo.client)(`ctx-${run}`).Whoami();
      expect(me).toMatchObject({ type: "Echo", name: `ctx-${run}`, attempt: 1 });
      expect(me.taskId).toBeString();
    }),
  ),
  timeout,
);

test(
  "primaryKey dedupes per agent; the same key on another agent is a different task",
  viaApi(
    Effect.gen(function* () {
      const echoes = yield* Echo.client;
      const a = echoes(`dedupe-a-${run}`);
      expect(yield* a.EchoKeyed({ id: "k", text: "one" })).toEqual({ echoed: "ONE" });
      // A settled task never runs again, and its outcome never changes.
      expect(yield* a.EchoKeyed({ id: "k", text: "two" })).toEqual({ echoed: "ONE" });
      expect((yield* record("Echo", `dedupe-a-${run}`, "EchoKeyed:k")).attempts).toBe(1);
      expect(yield* echoes(`dedupe-b-${run}`).EchoKeyed({ id: "k", text: "three" })).toEqual({ echoed: "THREE" });
    }),
  ),
  timeout,
);

test(
  "runs tasks FIFO",
  viaApi(
    Effect.gen(function* () {
      const echo = (yield* Echo.client)(`fifo-${run}`);
      for (const label of ["first", "second", "third"]) yield* echo.Order({ label }, { discard: true });
      const results = [];
      for (const label of ["first", "second", "third"]) results.push(yield* echo.Order({ label }));
      expect(results.map((r) => r.label)).toEqual(["first", "second", "third"]);
      expect(results[0]!.n < results[1]!.n && results[1]!.n < results[2]!.n).toBe(true);
    }),
  ),
  timeout,
);

test(
  "an undeclared failure is retried; waiting ignores non-final transitions",
  viaApi(
    Effect.gen(function* () {
      expect(yield* (yield* Flaky.client)(`retry-${run}`).FlakyTask({ failTimes: 1 })).toEqual({ attempt: 2 });
    }),
  ),
  timeout,
);

test(
  "retries until attempts are exhausted, then fails with AgentError(RetriesExhausted)",
  viaApi(
    Effect.gen(function* () {
      const error = yield* Effect.flip((yield* Flaky.client)(`exhaust-${run}`).FlakyTask({ failTimes: 10 }));
      expect(error).toBeInstanceOf(Agent.AgentError);
      expect(error.reason).toMatchObject({ _tag: "RetriesExhausted", attempts: 3, lastError: "flake #3" });
    }),
  ),
  timeout,
);

test(
  "a declared error settles the task on the first attempt and reaches the caller as itself",
  viaApi(
    Effect.gen(function* () {
      const error = yield* Effect.flip((yield* Flaky.client)(`refuse-${run}`).Refuse());
      expect(error).toBeInstanceOf(Nope);
      expect(error).toMatchObject({ _tag: "Nope", reason: "never" });
    }),
  ),
  timeout,
);

test(
  "a defect is retried like any undeclared failure",
  viaApi(
    Effect.gen(function* () {
      const error = yield* Effect.flip((yield* Flaky.client)(`die-${run}`).Die());
      expect(error.reason).toMatchObject({ _tag: "RetriesExhausted", attempts: 3, lastError: "boom" });
    }),
  ),
  timeout,
);

test(
  "a running task whose execution is lost is redelivered; the caller's wait survives the reset",
  viaApi(
    Effect.gen(function* () {
      expect(yield* (yield* Flaky.client)(`crash-${run}`).Crash({ times: 1 })).toEqual({ attempt: 2 });
    }),
  ),
  timeout,
);

test(
  "a task that keeps killing its execution ends up failed, not redelivered forever",
  viaApi(
    Effect.gen(function* () {
      const error = yield* Effect.flip((yield* Flaky.client)(`crashloop-${run}`).Crash({ times: 99 }));
      expect(error.reason).toMatchObject({ _tag: "ExecutionLost", attempts: 3 });
    }),
  ),
  timeout,
);

test(
  "an unknown task or invalid payload is stored as failed, not thrown",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const post = (body: unknown) =>
      HttpClient.execute(
        HttpClientRequest.post(`${url}/agents/Echo/bad-${run}/tasks`).pipe(
          HttpClientRequest.bearerToken(token),
          HttpClientRequest.setBody(HttpBody.jsonUnsafe(body)),
        ),
      ).pipe(Effect.flatMap((res) => Effect.map(res.json, (json) => ({ status: res.status, json }))));
    expect(yield* post({ tag: "Nope", payload: {}, taskId: "u" })).toMatchObject({
      status: 202,
      json: { status: "failed", attempts: 0, error: { kind: "agent", reason: { _tag: "UnknownTask", tag: "Nope" } } },
    });
    expect(yield* post({ tag: "EchoText", payload: { text: 1 }, taskId: "i" })).toMatchObject({
      status: 202,
      json: { status: "failed", attempts: 0, error: { kind: "agent", reason: { _tag: "InvalidPayload" } } },
    });
  }),
  timeout,
);

test(
  "success schemas encode on the wire and decode for the caller",
  viaApi(
    Effect.gen(function* () {
      const out = yield* (yield* Echo.client)(`stamp-${run}`).Stamp({ epochMs: 0 });
      expect(out.at).toBeInstanceOf(Date);
      expect(out.at.getTime()).toBe(0);
    }),
  ),
  timeout,
);

test(
  "an agent sends to another agent through its client, with causation",
  viaApi(
    Effect.gen(function* () {
      const { taskId } = yield* (yield* Relay.client)(`r-${run}`).Forward({ text: "via" });
      expect(taskId).toStartWith("EchoKeyed:");
      // The send was recorded with Forward's outcome and delivered after it, by the outbox.
      const rec = yield* settledRecord("Echo", `from-r-${run}`, taskId);
      expect(rec).toMatchObject({ status: "completed", result: { echoed: "VIA" } });
      expect(rec.causation).toStartWith(`Relay/r-${run}/`);
    }),
  ),
  timeout,
);

test(
  "a handler waits for another agent's answer without holding an execution",
  viaApi(
    Effect.gen(function* () {
      expect(yield* (yield* Relay.client)(`ask-${run}`).AskEcho({ text: "x" })).toBe("X");
    }),
  ),
  timeout,
);

test(
  "A → B → A: agents waiting on each other don't deadlock",
  viaApi(
    Effect.gen(function* () {
      const path = yield* (yield* Relay.client)(`v-${run}-a`).Volley({ n: 4, path: [] });
      expect(path).toEqual([`v-${run}-a`, `v-${run}-b`, `v-${run}-a`, `v-${run}-b`, `v-${run}-a`]);
    }),
  ),
  timeout,
);

test(
  "a declared error reaches the waiting handler as itself",
  viaApi(
    Effect.gen(function* () {
      expect(yield* (yield* Relay.client)(`refuse-${run}`).AskRefuse()).toBe("caught: never");
    }),
  ),
  timeout,
);

test(
  "parallel calls are all sent, once each, with ids derived from the calling task",
  viaApi(
    Effect.gen(function* () {
      const out = yield* (yield* Relay.client)(`fan-${run}`).FanOut({ key: "k", texts: ["a", "b", "c"] });
      expect(out).toEqual(["A", "B", "C"]);
      for (const i of [0, 1, 2]) {
        const rec = yield* record("Echo", `fan-${i}`, `Relay/fan-${run}/FanOut:k#c${i}`);
        expect(rec).toMatchObject({ status: "completed", attempts: 1, causation: `Relay/fan-${run}/FanOut:k` });
      }
    }),
  ),
  timeout,
);

test(
  "waiting on a keyed task that already settled answers at once, with the original result",
  viaApi(
    Effect.gen(function* () {
      const id = `k-${run}`;
      expect(yield* (yield* Echo.client)("keyed").EchoKeyed({ id, text: "first" })).toEqual({ echoed: "FIRST" });
      // Same key, same task: the handler gets the settled result, not a new one.
      expect(yield* (yield* Relay.client)(`keyed-${run}`).AskKeyed({ id, text: "second" })).toBe("FIRST");
    }),
  ),
  timeout,
);

test(
  "a handler that crashes after its answer arrived reruns with the recorded answer, without calling again",
  viaApi(
    Effect.gen(function* () {
      const out = yield* (yield* Relay.client)(`crash-${run}`).CrashAfterAnswer({ key: "k", text: "once" });
      expect(out).toEqual({ echoed: "ONCE", runs: 2 });
      const call = yield* record("Echo", "crash", `Relay/crash-${run}/CrashAfterAnswer:k#c0`);
      expect(call).toMatchObject({ status: "completed", attempts: 1 });
      const relay = yield* record("Relay", `crash-${run}`, "CrashAfterAnswer:k");
      expect(relay).toMatchObject({ status: "completed", attempts: 2 });
    }),
  ),
  timeout,
);

test(
  "a Worker's own route calls an agent through its client (the API is mounted alongside)",
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
        HttpClientRequest.get(`${url}/agents/Echo/a/tasks/x`).pipe(auth ? HttpClientRequest.setHeader("authorization", auth) : (r) => r),
      ).pipe(Effect.map((res) => res.status));
    expect(yield* status()).toBe(401);
    expect(yield* status("Bearer wrong")).toBe(401);
    expect(yield* status(`Bearer ${token}`)).toBe(404);
  }),
  timeout,
);
