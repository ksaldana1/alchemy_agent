// LLM agents on a scripted model. Local by default; `bun run test:cloud` runs it on test_$USER.
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
  Effect.flatMap(stack, ({ url }) => effect.pipe(Effect.provide(Api.connect({ url: url!, token }))));

const getText = (path: string) =>
  Effect.flatMap(stack, ({ url }) =>
    HttpClient.execute(HttpClientRequest.get(`${url}${path}`).pipe(HttpClientRequest.bearerToken(token))).pipe(Effect.flatMap((r) => r.text)),
  );
type Entry = { seq: number; kind: string; message: { content: unknown } };
const transcript = (agent: string, session: string) =>
  Effect.map(getText(`/agents/Desk/${agent}/sessions/${encodeURIComponent(session)}`), (t) => JSON.parse(t) as Entry[]);
const desk = (name: string) => Effect.map(Desk.client, (clients) => clients(name));

const timeout = { timeout: 60_000 };

test(
  "a generate turn with a tool call, persisted step by step",
  viaApi(
    Effect.gen(function* () {
      const { url } = yield* stack;
      yield* Test.getWhenReady(`${url}/shout/warmup`);
      const d = yield* desk(`a-${run}`);
      expect(yield* d.Ask({ message: "What is the status of order 42?", session: "s1" })).toBe("Order 42 has shipped.");
      expect((yield* transcript(`a-${run}`, "s1")).map((e) => e.kind)).toEqual(["user", "model", "tool_result", "model"]);
      expect(yield* d.Counter({ name: "lookup:42" })).toBe(1);
    }),
  ),
  timeout,
);

test(
  "a crash mid-turn resumes at the failed step; earlier tool calls don't run again",
  viaApi(
    Effect.gen(function* () {
      const d = yield* desk(`chaos-${run}`);
      expect(yield* d.Ask({ message: "chaos please", session: "c" })).toBe("done after chaos");
      expect(yield* d.Counter({ name: "lookup:7" })).toBe(1); // the earlier tool: once
      expect(yield* d.Counter({ name: "crashy" })).toBe(2); // the crashing tool: at least once, so twice
      expect((yield* transcript(`chaos-${run}`, "c")).map((e) => e.kind)).toEqual([
        "user",
        "model",
        "tool_result",
        "model",
        "tool_result",
        "model",
      ]);
    }),
  ),
  timeout,
);

test(
  "a tool delegates to another agent: the turn waits for its answer, then finishes with it",
  viaApi(
    Effect.gen(function* () {
      const d = yield* desk(`delegate-${run}`);
      expect(yield* d.Ask({ message: "delegate this", session: "d" })).toBe("Echo answered PSST");
      const entries = yield* transcript(`delegate-${run}`, "d");
      expect(entries.map((e) => e.kind)).toEqual(["user", "model", "tool_result", "model"]);
      expect(JSON.stringify(entries[2]!.message)).toContain("PSST");
    }),
  ),
  timeout,
);

test(
  "bad tool arguments become a failed tool result the model sees, not a failed turn",
  viaApi(
    Effect.gen(function* () {
      expect(yield* (yield* desk(`args-${run}`)).Ask({ message: "bad args please" })).toBe("fixed after error");
    }),
  ),
  timeout,
);

test(
  "generate with a struct schema, inside a handler that keeps going after it",
  viaApi(
    Effect.gen(function* () {
      // The handler ran before the turn (and suspended), then again after it (and finished).
      expect(yield* (yield* desk(`review-${run}`)).Review({ article: "budget" })).toEqual({
        approved: true,
        notes: "reads well",
        handlerRuns: 2,
      });
    }),
  ),
  timeout,
);

test(
  "a structured answer is forced through `respond` when the model replies in text",
  viaApi(
    Effect.gen(function* () {
      expect(yield* (yield* desk(`talk-${run}`)).ReviewTalkative({ article: "x" })).toEqual({ approved: false, notes: "forced" });
    }),
  ),
  timeout,
);

test(
  "an invalid structured answer goes back to the model, which answers again",
  viaApi(
    Effect.gen(function* () {
      expect(yield* (yield* desk(`sloppy-${run}`)).ReviewSloppy({ article: "x" })).toEqual({ approved: true, notes: "repaired" });
    }),
  ),
  timeout,
);

test(
  "generate is typed by its schema: literals through `respond`, string-encoded values from text",
  viaApi(
    Effect.gen(function* () {
      const d = yield* desk(`poly-${run}`);
      expect(yield* d.Pick({ question: "ship it?" })).toBe("reject");
      // "lots" doesn't decode as a number: the model is asked again, and "42" does.
      expect(yield* d.Count({ question: "mugs?" })).toBe(42);
    }),
  ),
  timeout,
);

test(
  "a turn that never finishes hits the step cap: the task fails with GenerationFailed",
  viaApi(
    Effect.gen(function* () {
      const error = yield* Effect.flip((yield* desk(`loop-${run}`)).Ask({ message: "loop forever" }));
      expect(error.reason._tag).toBe("GenerationFailed");
      expect(error.reason).toMatchObject({ message: expect.stringContaining("StepLimit") });
    }),
  ),
  timeout,
);

test(
  "empty model responses (swallowed provider errors) are retried, then fail the turn",
  viaApi(
    Effect.gen(function* () {
      const error = yield* Effect.flip((yield* desk(`empty-${run}`)).Ask({ message: "empty please" }));
      expect(error.reason).toMatchObject({ _tag: "GenerationFailed", message: expect.stringContaining("empty response") });
    }),
  ),
  timeout,
);

test(
  "a session remembers earlier turns",
  viaApi(
    Effect.gen(function* () {
      const d = yield* desk(`chat-${run}`);
      yield* d.Ask({ message: "my name is Ada", session: "chat" });
      expect(yield* d.Ask({ message: "what is my name?", session: "chat" })).toBe("Your name is Ada.");
      expect(yield* d.Ask({ message: "what is my name?", session: "other" })).toBe("I don't know.");
    }),
  ),
  timeout,
);

test(
  "turns in one session run one at a time",
  viaApi(
    Effect.gen(function* () {
      const d = yield* desk(`queue-${run}`);
      const [a, b] = yield* Effect.all([d.Ask({ message: "slow one", session: "q" }), d.Ask({ message: "slow two", session: "q" })], {
        concurrency: "unbounded",
      });
      expect([a, b]).toEqual(["echo: slow one", "echo: slow two"]);
      // Never interleaved: each user message is answered before the next one starts.
      expect((yield* transcript(`queue-${run}`, "q")).map((e) => e.kind)).toEqual(["user", "model", "user", "model"]);
    }),
  ),
  timeout,
);

test(
  "the events endpoint replays the transcript as SSE, then ends when the session is idle",
  viaApi(
    Effect.gen(function* () {
      yield* (yield* desk(`sse-${run}`)).Ask({ message: "What is the status of order 42?", session: "e" });
      const body = yield* getText(`/agents/Desk/sse-${run}/sessions/e/events`);
      const events = body.split("\n\n").filter(Boolean).map((chunk) => chunk.match(/^event: (.*)$/m)?.[1] ?? "message"); // SSE's default event
      expect(events).toEqual(["message", "message", "message", "message", "idle"]);
      const after = yield* getText(`/agents/Desk/sse-${run}/sessions/e/events?after=2`);
      expect(after.match(/^id: (\d+)$/gm)).toEqual(["id: 3", "id: 4"]);
    }),
  ),
  timeout,
);
