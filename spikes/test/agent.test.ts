import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import Stack from "../alchemy.run.ts";

// Spike 0.2 runtime checks. Cloud by default (test_$USER); ALCHEMY_TEST_DEV=1 runs locally.
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

// Fresh agent names per run: deployed DO storage outlives a test run.
const run = crypto.randomUUID().slice(0, 8);

const post = (url: string, body: unknown) =>
  Test.executeWhenReady(
    HttpClientRequest.post(url).pipe(HttpClientRequest.setBody(HttpBody.jsonUnsafe(body))),
  ).pipe(Effect.flatMap((res) => Effect.map(res.json, (json) => ({ status: res.status, json }))));

const get = (url: string) =>
  HttpClient.get(url).pipe(
    Effect.flatMap((res) => Effect.map(res.json, (json) => ({ status: res.status, json }))),
  );

test(
  "Worker → Echo through the typed stub (approach A)",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const base = `${url}/echo/a-${run}/tasks`;

    const sent = yield* post(base, { taskId: "t1", task: "echo", input: { text: "hi" } });
    expect(sent).toEqual({ status: 200, json: { taskId: "t1", task: "echo" } });
    expect((yield* get(`${base}/t1`)).json).toEqual({
      status: "completed",
      task: "echo",
      result: { echoed: "HI" },
    });

    // AgentContext: the DO knows its own agent name (state.id.name).
    yield* post(base, { taskId: "t2", task: "count", input: { n: 7 } });
    expect((yield* get(`${base}/t2`)).json).toEqual({
      status: "completed",
      task: "count",
      result: { n: 7, by: `a-${run}` },
    });

    // Same task id again is a no-op: the stored result doesn't change.
    yield* post(base, { taskId: "t1", task: "echo", input: { text: "other" } });
    expect((yield* get(`${base}/t1`)).json).toMatchObject({ result: { echoed: "HI" } });
  }),
  { timeout: 60_000 },
);

test(
  "tagged errors from the DO arrive as { _tag } objects that catchTag still matches",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const base = `${url}/echo/err-${run}/tasks`;
    const unknown = yield* post(base, { taskId: "u", task: "nope", input: {} });
    expect(unknown.status).toBe(400);
    expect(unknown.json).toMatchObject({ _tag: "UnknownTaskError", task: "nope" });

    const bad = yield* post(base, { taskId: "b", task: "echo", input: { text: 1 } });
    expect(bad.status).toBe(400);
    expect(bad.json).toMatchObject({ _tag: "TaskInputError", task: "echo" });
  }),
  { timeout: 60_000 },
);

test(
  "Relay → Echo: one agent calls another (approach C client inside a DO)",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const base = `${url}/relay/r-${run}/tasks`;

    yield* post(base, { taskId: "r1", task: "relay", input: { text: "via relay" } });
    expect((yield* get(`${base}/r1`)).json).toEqual({
      status: "completed",
      task: "relay",
      result: {
        via: `r-${run}`,
        record: { status: "completed", task: "echo", result: { echoed: "VIA RELAY" } },
      },
    });

    // The Echo agent that Relay called holds the task under Relay's derived id.
    expect((yield* get(`${url}/echo/relay-r-${run}/tasks/r1:echo`)).json).toMatchObject({
      status: "completed",
    });

    // A handler's PermanentError is stored with its tag.
    yield* post(base, { taskId: "f1", task: "fail", input: { reason: "no" } });
    expect((yield* get(`${base}/f1`)).json).toEqual({
      status: "failed",
      task: "fail",
      error: { _tag: "PermanentError", message: "no" },
    });

    // A declared output schema stores the Encoded form (Date → ISO string).
    yield* post(base, { taskId: "s1", task: "stamp", input: { epochMs: 0 } });
    expect((yield* get(`${base}/s1`)).json).toEqual({
      status: "completed",
      task: "stamp",
      result: { at: "1970-01-01T00:00:00.000Z" },
    });

    // The client validates before the round trip.
    const bad = yield* post(base, { taskId: "x", task: "stamp", input: { epochMs: "0" } });
    expect(bad.json).toMatchObject({ _tag: "TaskInputError", task: "stamp" });
  }),
  { timeout: 60_000 },
);
