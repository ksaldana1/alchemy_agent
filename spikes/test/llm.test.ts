// Spike 0.5 / L4 + L8: the step-per-callback turn with the scripted model (no network). It fails
// once right after a tool result is stored; the retry must not run the tool again.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import Stack from "../alchemy.run.ts";

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});
const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

type Status = {
  session: { done: number; chaos_done: number } | null;
  transcript: Array<{ kind: string }>;
  toolExecutions: number;
};

test(
  "a scripted turn survives a failure after a tool result, without re-running the tool",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const session = `t-${crypto.randomUUID().slice(0, 8)}`;
    yield* Test.executeWhenReady(
      HttpClientRequest.post(`${url}/llm/${session}/start`).pipe(
        HttpClientRequest.setBody(HttpBody.jsonUnsafe(["What is the status of order 42?", "scripted", "", true])),
      ),
    );
    const status = yield* HttpClient.get(`${url}/llm/${session}/status`).pipe(
      Effect.flatMap((r) => r.json),
      Effect.map((j) => j as unknown as Status),
      Effect.repeat({ schedule: Schedule.spaced("300 millis"), until: (s) => s.session?.done === 1, times: 60 }),
    );
    expect(status.session?.chaos_done).toBe(1);
    expect(status.transcript.map((t) => t.kind)).toEqual(["user", "model", "tool_result", "model"]);
    expect(status.toolExecutions).toBe(1);
  }),
  { timeout: 60_000 },
);
