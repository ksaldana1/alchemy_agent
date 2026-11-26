// M2: LLM agents on a scripted model. Local by default; `bun run test:cloud` runs it on test_$USER.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Api from "../src/Api.ts";
import { Desk } from "./fixtures/llm.ts";
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
const viaApi = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.flatMap(stack, ({ url }) => effect.pipe(Effect.provide(Api.connect({ url: url!, token }, Desk))));

const getJson = (path: string) =>
  Effect.flatMap(stack, ({ url }) =>
    HttpClient.execute(HttpClientRequest.get(`${url}${path}`).pipe(HttpClientRequest.bearerToken(token))).pipe(
      Effect.flatMap((r) => r.text),
    ),
  );
type Entry = { seq: number; kind: string; message: { content: unknown } };
const transcript = (agent: string, session: string) =>
  Effect.map(getJson(`/agents/Desk/${agent}/sessions/${encodeURIComponent(session)}`), (t) => JSON.parse(t) as Entry[]);
/** How many times `counter` was bumped in agent `agent`'s Durable Object. */
const counted = (agent: string, counter: string) =>
  Effect.map(Desk.named(agent).counter({ name: counter }), (r) => r.n);

const timeout = { timeout: 60_000 };

test(
  "the built-in prompt task: one turn with a tool call, persisted step by step",
  viaApi(
    Effect.gen(function* () {
      const { url } = yield* stack;
      yield* Test.getWhenReady(`${url}/shout/warmup`);
      const desk = Desk.named(`a-${run}`);
      expect(yield* desk.prompt({ message: "What is the status of order 42?", session: "s1" })).toEqual({
        text: "Order 42 has shipped.",
      });
      const t = yield* transcript(`a-${run}`, "s1");
      expect(t.map((e) => e.kind)).toEqual(["user", "model", "tool_result", "model"]);
      expect(yield* counted(`a-${run}`, "lookup:42")).toBe(1);
    }),
  ),
  timeout,
);

test(
  "a crash mid-turn resumes at the failed step; earlier tool calls don't run again",
  viaApi(
    Effect.gen(function* () {
      const desk = Desk.named(`chaos-${run}`);
      expect(yield* desk.prompt({ message: "chaos please", session: "c" })).toEqual({ text: "done after chaos" });
      expect(yield* counted(`chaos-${run}`, "lookup:7")).toBe(1); // the earlier tool: once
      expect(yield* counted(`chaos-${run}`, "crashy")).toBe(2); // the crashing tool: at least once, so twice
      const t = yield* transcript(`chaos-${run}`, "c");
      expect(t.map((e) => e.kind)).toEqual(["user", "model", "tool_result", "model", "tool_result", "model"]);
    }),
  ),
  timeout,
);

test(
  "bad tool arguments become a failed tool result the model sees, not a failed turn",
  viaApi(
    Effect.gen(function* () {
      expect(yield* Desk.named(`args-${run}`).prompt({ message: "bad args please" })).toEqual({ text: "fixed after error" });
    }),
  ),
  timeout,
);

test(
  "Agent.prompt with an output schema, inside a task that keeps going after the prompt",
  viaApi(
    Effect.gen(function* () {
      const out = yield* Desk.named(`review-${run}`).review({ article: "budget" });
      // The handler ran before the turn (and suspended), then again after it (and finished).
      expect(out).toEqual({ approved: true, notes: "reads well", handlerRuns: 2 });
    }),
  ),
  timeout,
);

test(
  "a structured answer is forced through `respond` when the model replies in text",
  viaApi(
    Effect.gen(function* () {
      expect(yield* Desk.named(`talk-${run}`).reviewTalkative({ article: "x" })).toEqual({ approved: false, notes: "forced" });
    }),
  ),
  timeout,
);

test(
  "an invalid structured answer goes back to the model, which answers again",
  viaApi(
    Effect.gen(function* () {
      expect(yield* Desk.named(`sloppy-${run}`).reviewSloppy({ article: "x" })).toEqual({ approved: true, notes: "repaired" });
    }),
  ),
  timeout,
);

test(
  "a turn that never finishes hits the step cap: PromptFailed settles the task",
  viaApi(
    Effect.gen(function* () {
      const failed = yield* Effect.flip(Desk.named(`loop-${run}`).prompt({ message: "loop forever" }));
      expect(failed).toMatchObject({ _tag: "TaskFailed", attempts: 1, error: { _tag: "PromptFailed" } });
      expect(failed._tag === "TaskFailed" && failed.error.message).toContain("StepLimit");
    }),
  ),
  timeout,
);

test(
  "empty model responses (swallowed provider errors) are retried, then fail the turn",
  viaApi(
    Effect.gen(function* () {
      const failed = yield* Effect.flip(Desk.named(`empty-${run}`).prompt({ message: "empty please" }));
      expect(failed).toMatchObject({ _tag: "TaskFailed", error: { _tag: "PromptFailed" } });
      expect(failed._tag === "TaskFailed" && failed.error.message).toContain("empty response");
    }),
  ),
  timeout,
);

test(
  "a session remembers earlier turns",
  viaApi(
    Effect.gen(function* () {
      const desk = Desk.named(`chat-${run}`);
      yield* desk.prompt({ message: "my name is Ada", session: "chat" });
      expect(yield* desk.prompt({ message: "what is my name?", session: "chat" })).toEqual({ text: "Your name is Ada." });
      expect(yield* desk.prompt({ message: "what is my name?", session: "other" })).toEqual({ text: "I don't know." });
    }),
  ),
  timeout,
);

test(
  "turns in one session run one at a time, in order",
  viaApi(
    Effect.gen(function* () {
      const desk = Desk.named(`queue-${run}`);
      const a = yield* desk.send.prompt({ message: "slow one", session: "q" });
      const b = yield* desk.send.prompt({ message: "slow two", session: "q" });
      expect(yield* a.result).toEqual({ text: "echo: slow one" });
      expect(yield* b.result).toEqual({ text: "echo: slow two" });
      const t = yield* transcript(`queue-${run}`, "q");
      expect(t.map((e) => `${e.kind}:${JSON.stringify(e.message.content)}`)).toEqual([
        `user:"slow one"`,
        `model:"echo: slow one"`,
        `user:"slow two"`,
        `model:"echo: slow two"`,
      ]);
    }),
  ),
  timeout,
);

test(
  "the events endpoint replays the transcript as SSE, then ends when the session is idle",
  viaApi(
    Effect.gen(function* () {
      yield* Desk.named(`sse-${run}`).prompt({ message: "What is the status of order 42?", session: "e" });
      const body = yield* getJson(`/agents/Desk/sse-${run}/sessions/e/events`);
      const events = body.split("\n\n").filter(Boolean).map((chunk) => chunk.match(/^event: (.*)$/m)?.[1]);
      expect(events).toEqual(["message", "message", "message", "message", "idle"]);
      const after = yield* getJson(`/agents/Desk/sse-${run}/sessions/e/events?after=2`);
      expect(after.match(/^id: (\d+)$/gm)).toEqual(["id: 3", "id: 4"]);
    }),
  ),
  timeout,
);
