// Spike 0.5 / L3: does an Effect AI Prompt survive JSON losslessly (tool call ids included)?
// Pure: no Alchemy, no network. `bun test spikes/test/prompt-roundtrip.test.ts`.
import { describe, expect, test } from "bun:test";
import * as Prompt from "effect/ai/Prompt";
import * as Response from "effect/ai/Response";
import * as Schema from "effect/Schema";

const encode = Schema.encodeSync(Prompt.Prompt);
const decode = Schema.decodeUnknownSync(Prompt.Prompt);
const roundTrip = (p: Prompt.Prompt) => decode(JSON.parse(JSON.stringify(encode(p))));

const turn = Prompt.fromMessages([
  Prompt.systemMessage({ content: "You are a support agent." }),
  Prompt.userMessage({ content: [Prompt.textPart({ text: "What's the status of order 42?" })] }),
  Prompt.assistantMessage({
    content: [
      Prompt.textPart({ text: "Let me look that up." }),
      Prompt.toolCallPart({ id: "call_abc123", name: "lookup_order", params: { orderId: "42" }, providerExecuted: false }),
      Prompt.toolCallPart({ id: "call_def456", name: "current_time", params: {}, providerExecuted: false }),
    ],
  }),
  Prompt.toolMessage({
    content: [
      Prompt.toolResultPart({
        id: "call_abc123",
        name: "lookup_order",
        isFailure: false,
        providerExecuted: false,
        result: { orderId: "42", status: "shipped", items: [{ sku: "A-1", qty: 2 }] },
      }),
      Prompt.toolResultPart({ id: "call_def456", name: "current_time", isFailure: false, providerExecuted: false, result: "2026-11-25T10:00:00.000Z" }),
    ],
  }),
  Prompt.assistantMessage({ content: [Prompt.textPart({ text: "Order 42 has shipped." })] }),
]);

describe("L3: Prompt JSON round trip", () => {
  test("encode → JSON → decode is deep-equal, tool call ids included", () => {
    const back = roundTrip(turn);
    expect(back).toEqual(turn);
    expect(encode(back)).toEqual(encode(turn));
    const ids = back.content.flatMap((m) =>
      typeof m.content === "string" ? [] : m.content.flatMap((p) => ("id" in p ? [p.id] : [])),
    );
    expect(ids).toEqual(["call_abc123", "call_def456", "call_abc123", "call_def456"]);
  });

  test("messages round-trip one at a time (one transcript row per message)", () => {
    const encodeMessage = Schema.encodeSync(Prompt.Message);
    const decodeMessage = Schema.decodeUnknownSync(Prompt.Message);
    const rows = turn.content.map((m) => JSON.stringify(encodeMessage(m)));
    const rebuilt = Prompt.fromMessages(rows.map((r) => decodeMessage(JSON.parse(r))));
    expect(rebuilt).toEqual(turn);
  });

  test("a model response's parts become an assistant message (Prompt.fromResponseParts)", () => {
    const parts = [
      Response.makePart("text", { text: "Checking." }),
      Response.makePart("tool-call", { id: "call_x", name: "lookup_order", params: { orderId: "7" }, providerExecuted: false }),
    ] as unknown as ReadonlyArray<Response.AnyPart>;
    const asPrompt = Prompt.fromResponseParts(parts);
    const back = roundTrip(asPrompt);
    expect(back).toEqual(asPrompt);
    expect(JSON.stringify(encode(back))).toContain("call_x");
  });
});
