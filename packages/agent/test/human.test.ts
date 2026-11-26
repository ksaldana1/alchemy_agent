// Human requests (M7): a person answers over HTTP, and the waiting workflow or task resumes once.
// An answer must match the request's schema, and an approval applies only to the revision it was
// asked for. Also: listing an agent's inbox (inspection).
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

type Request = {
  id: string;
  name: string;
  prompt: string;
  status: string;
  artifact: { path: string; revision: string } | null;
  causation: string | null;
  answer?: { response: unknown; by: string; revision: string | null };
};

/** Any status code is an answer here (no retrying 4xx). */
const call = (request: HttpClientRequest.HttpClientRequest) =>
  Effect.flatMap(stack, ({ url }) =>
    HttpClient.execute(request.pipe(HttpClientRequest.prependUrl(url!), HttpClientRequest.bearerToken(token))).pipe(
      Effect.flatMap((res) => Effect.map(res.text, (text) => ({ status: res.status, body: (text === "" ? null : JSON.parse(text)) as any }))),
    ),
  );
const ready = Effect.flatMap(stack, ({ url }) => Test.getWhenReady(`${url}/shout/warmup`));
const post = (path: string, body: unknown) => call(HttpClientRequest.post(path).pipe(HttpClientRequest.setBody(HttpBody.jsonUnsafe(body))));
const get = (path: string) => call(HttpClientRequest.get(path));

const startReview = (key: string, timeoutMs = 120_000) =>
  post(`/workflows/HumanReview/runs`, { payload: { key, timeoutMs } }).pipe(Effect.map((r) => r.body.executionId as string));
const runStatus = (id: string) => get(`/workflows/HumanReview/runs/${id}`).pipe(Effect.map((r) => r.body));
const finished = (id: string) =>
  runStatus(id).pipe(Effect.repeat({ schedule: Schedule.spaced("250 millis"), until: (s) => s._tag !== "Running", times: 400 }));
/** The pending request a run (or task) made, once it exists. */
const requestFrom = (causation: string) =>
  get(`/requests`).pipe(
    Effect.map((r) => (r.body as ReadonlyArray<Request>).find((q) => q.causation === causation)),
    Effect.repeat({ schedule: Schedule.spaced("250 millis"), until: (q) => q !== undefined, times: 200 }),
    Effect.map((q) => q!),
  );
const respond = (id: string, body: unknown) => post(`/requests/${encodeURIComponent(id)}/respond`, body);

test(
  "a person approves a revision over HTTP; the waiting run resumes once; a second answer is a no-op",
  Effect.gen(function* () {
    yield* ready;
    const id = yield* startReview(`ok-${run}`);
    const request = yield* requestFrom(`workflow/HumanReview/${id}`);
    expect(request).toMatchObject({ name: "sign-off", status: "pending", artifact: { path: `agents/Filer/drafter-ok-${run}/drafts/ok-${run}.md` } });
    const revision = request.artifact!.revision;

    const first = yield* respond(request.id, { response: { approved: true, notes: "ship it" }, by: "ada", revision });
    expect(first.status).toBe(200);
    expect(first.body).toMatchObject({ status: "resolved", answer: { by: "ada", revision } });
    expect(yield* finished(id)).toEqual({
      _tag: "Completed",
      result: { outcome: "approved", by: "ada", path: request.artifact!.path, revision },
    });

    // A duplicate (or a different) answer changes nothing.
    const again = yield* respond(request.id, { response: { approved: false }, by: "eve", revision });
    expect(again.status).toBe(409);
    expect(again.body).toMatchObject({ error: "already settled", request: { status: "resolved", answer: { by: "ada" } } });
    expect((yield* runStatus(id)).result.by).toBe("ada");
  }),
  timeout,
);

test(
  "an approval for another revision, or after the file changed, is rejected as stale",
  Effect.gen(function* () {
    const id = yield* startReview(`stale-${run}`);
    const request = yield* requestFrom(`workflow/HumanReview/${id}`);
    const { path, revision } = request.artifact!;

    // Answering for a revision the request isn't about.
    const wrong = yield* respond(request.id, { response: { approved: true }, by: "ada", revision: "not-it" });
    expect(wrong.status).toBe(409);
    expect(wrong.body.error).toBe("stale");

    // The file changes after the request: the old revision can't be approved any more.
    yield* call(HttpClientRequest.put(`/workspace/files/${path}`).pipe(HttpClientRequest.setBody(HttpBody.text("changed"))));
    const stale = yield* respond(request.id, { response: { approved: true }, by: "ada", revision });
    expect(stale.status).toBe(409);
    expect(stale.body.message).toMatch(/changed since the request/);
    expect((yield* get(`/requests/${encodeURIComponent(request.id)}`)).body.status).toBe("pending");

    // It can still be canceled; the run sees the cancellation.
    expect((yield* post(`/requests/${encodeURIComponent(request.id)}/cancel`, { by: "ops", reason: "stale draft" })).status).toBe(200);
    expect(yield* finished(id)).toMatchObject({ _tag: "Completed", result: { outcome: "canceled", by: "ops" } });
  }),
  timeout,
);

test(
  "a response that doesn't match the request's schema is a 400, and the request stays pending",
  Effect.gen(function* () {
    const id = yield* startReview(`bad-${run}`);
    const request = yield* requestFrom(`workflow/HumanReview/${id}`);
    const bad = yield* respond(request.id, { response: { approved: "yes" }, by: "ada", revision: request.artifact!.revision });
    expect(bad.status).toBe(400);
    expect((yield* respond(request.id, { response: { approved: true }, revision: request.artifact!.revision })).status).toBe(400); // no `by`
    const no = yield* respond(request.id, { response: { approved: false, notes: "typos" }, by: "bob", revision: request.artifact!.revision });
    expect(no.status).toBe(200);
    expect(yield* finished(id)).toMatchObject({ _tag: "Completed", result: { outcome: "rejected", by: "bob" } });
  }),
  timeout,
);

test(
  "an unanswered request expires at its timeout",
  Effect.gen(function* () {
    const id = yield* startReview(`late-${run}`, 1_500);
    expect(yield* finished(id)).toMatchObject({ _tag: "Completed", result: { outcome: "expired", by: null } });
    const all = (yield* get(`/requests?status=expired`)).body as ReadonlyArray<Request>;
    const request = all.find((q) => q.causation === `workflow/HumanReview/${id}`)!;
    expect(request.status).toBe("expired");
    expect((yield* respond(request.id, { response: { approved: true }, by: "ada", revision: request.artifact!.revision })).status).toBe(409);
  }),
  timeout,
);

test(
  "a handler asks a person: its task waits, and the typed answer resumes it",
  Effect.gen(function* () {
    const key = `budget-${run}`;
    yield* post(`/agents/Clerk/clerk-${run}/tasks`, { tag: "AskBudget", payload: { key, timeoutMs: 120_000 }, taskId: `AskBudget:${key}` });
    const request = yield* requestFrom(`Clerk/clerk-${run}/AskBudget:${key}`);
    expect(request).toMatchObject({ name: "budget", prompt: `What's the budget for ${key}?`, artifact: null });
    expect((yield* get(`/agents/Clerk/clerk-${run}/tasks/${encodeURIComponent(`AskBudget:${key}`)}`)).body.status).toBe("waiting");
    expect((yield* respond(request.id, { response: { amount: "lots" }, by: "cfo" })).status).toBe(400);
    expect((yield* respond(request.id, { response: { amount: 42 }, by: "cfo" })).status).toBe(200);
    const done = yield* get(`/agents/Clerk/clerk-${run}/tasks/${encodeURIComponent(`AskBudget:${key}`)}?wait=1`).pipe(
      Effect.repeat({ schedule: Schedule.spaced("250 millis"), until: (r) => r.body.status === "completed", times: 120 }),
    );
    // The handler ran twice (before and after the answer) and finished once.
    expect(done.body).toMatchObject({ status: "completed", attempts: 1, result: { amount: 42, runs: 2 } });
  }),
  timeout,
);

test(
  "an agent's inbox can be listed, filtered by status; GET /agents lists the agents that have had work",
  Effect.gen(function* () {
    const name = `inbox-${run}`;
    for (const text of ["a", "b"]) yield* post(`/agents/Echo/${name}/tasks`, { tag: "EchoText", payload: { text }, taskId: text });
    yield* post(`/agents/Echo/${name}/tasks`, { tag: "Nope", payload: {}, taskId: "unknown" });
    const tasks = yield* get(`/agents/Echo/${name}/tasks`).pipe(
      Effect.repeat({ schedule: Schedule.spaced("250 millis"), until: (r) => r.body.filter((t: any) => t.status === "completed").length === 2, times: 80 }),
    );
    expect(tasks.body.map((t: any) => [t.taskId, t.status])).toEqual([
      ["a", "completed"],
      ["b", "completed"],
      ["unknown", "failed"],
    ]);
    const failed = yield* get(`/agents/Echo/${name}/tasks?status=failed`);
    expect(failed.body.map((t: any) => t.taskId)).toEqual(["unknown"]);
    // The registry knows the agent (it reported itself with its first task).
    const agents = (yield* get(`/agents?type=Echo`)).body as ReadonlyArray<{ type: string; name: string; firstSeen: number }>;
    expect(agents.find((a) => a.name === name)).toMatchObject({ type: "Echo", name });
    expect((yield* get(`/agents`)).body.some((a: any) => a.type === "AgentHumans" && a.name === "people")).toBe(true);
  }),
  timeout,
);
