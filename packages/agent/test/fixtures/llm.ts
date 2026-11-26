// LLM test agent on a scripted model: deterministic, no network.
import * as Prompt from "effect/ai/Prompt";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";
import * as Agent from "../../src/Agent.ts";
import * as AgentModel from "../../src/AgentModel.ts";
import * as Testing from "../../src/Testing.ts";

// ---------------------------------------------------------------------------
// Contract
// ---------------------------------------------------------------------------

export const Verdict = Schema.Struct({ approved: Schema.Boolean, notes: Schema.String });

export const Ask = Rpc.make("Ask", {
  payload: { message: Schema.String, session: Schema.optionalKey(Schema.String) },
  success: Schema.String,
});
export const Review = Rpc.make("Review", {
  payload: { article: Schema.String },
  success: Schema.Struct({ ...Verdict.fields, handlerRuns: Schema.Number }),
});
export const ReviewTalkative = Rpc.make("ReviewTalkative", { payload: { article: Schema.String }, success: Verdict });
export const ReviewSloppy = Rpc.make("ReviewSloppy", { payload: { article: Schema.String }, success: Verdict });
export const Pick = Rpc.make("Pick", { payload: { question: Schema.String }, success: Schema.Literals(["approve", "reject"]) });
export const Count = Rpc.make("Count", { payload: { question: Schema.String }, success: Schema.Number });
/** How many times a counter was bumped, read from the durable counters. */
export const Counter = Rpc.make("Counter", { payload: { name: Schema.String }, success: Schema.Number });

export const Desk = Agent.make("Desk", [Ask, Review, ReviewTalkative, ReviewSloppy, Pick, Count, Counter]);

// ---------------------------------------------------------------------------
// Tools (an Effect AI toolkit; handlers as a Layer)
// ---------------------------------------------------------------------------

const LookupOrder = Tool.make("lookup_order", {
  description: "Look up an order by id.",
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ orderId: Schema.String, status: Schema.String }),
});
const Crashy = Tool.make("crashy", {
  description: "Crashes the execution the first time it runs.",
  success: Schema.Struct({ runs: Schema.Number }),
  // Tools can depend on framework services; the framework provides them per call.
  dependencies: [Agent.ToolCall],
});
export const Tools = Toolkit.make(LookupOrder, Crashy);

export const ToolsLive = Tools.toLayer(
  Tools.of({
    lookup_order: ({ orderId }) => Effect.map(Testing.count(`lookup:${orderId}`), () => ({ orderId, status: "shipped" })),
    crashy: () =>
      Effect.gen(function* () {
        yield* Agent.ToolCall; // proves the dependency is provided
        const runs = yield* Testing.count("crashy");
        if (runs === 1) return yield* Testing.crashExecution;
        return { runs };
      }),
  }),
);

// ---------------------------------------------------------------------------
// The scripted model: replies chosen by keyword in the latest user message
// ---------------------------------------------------------------------------

/** The last user message, and how many assistant messages answered it so far. */
const latest = (prompt: Prompt.Prompt) => {
  const msgs = prompt.content;
  let i = msgs.length - 1;
  while (i >= 0 && msgs[i]!.role !== "user") i--;
  const user = msgs[i];
  const text = user === undefined ? "" : typeof user.content === "string" ? user.content : user.content.map((p) => ("text" in p ? p.text : "")).join("");
  return { text, n: msgs.slice(i + 1).filter((m) => m.role === "assistant").length, all: JSON.stringify(msgs) };
};

const script = (prompt: Prompt.Prompt): AgentModel.ScriptedResponse => {
  const { text, n, all } = latest(prompt);
  // The engine's repair request for a text answer that didn't decode (only the count test hits it).
  if (text.startsWith("That answer is invalid")) return AgentModel.text("42");
  if (text.includes("order 42")) return n === 0 ? AgentModel.toolCall("lookup_order", { orderId: "42" }) : AgentModel.text("Order 42 has shipped.");
  if (text.includes("chaos")) {
    return n === 0
      ? AgentModel.toolCall("lookup_order", { orderId: "7" })
      : n === 1
        ? AgentModel.toolCall("crashy", {})
        : AgentModel.text("done after chaos");
  }
  if (text.includes("bad args")) return n === 0 ? AgentModel.toolCall("lookup_order", { orderId: 42 }) : AgentModel.text(all.includes("Expected string") ? "fixed after error" : "no error seen");
  if (text.includes("sloppy")) return n === 0 ? AgentModel.respond({ approved: "yes", notes: "x" }) : AgentModel.respond({ approved: true, notes: all.includes("Invalid answer") ? "repaired" : "?" });
  if (text.includes("talkative")) return n === 0 ? AgentModel.text("I think it is fine.") : AgentModel.respond({ approved: false, notes: "forced" });
  if (text.includes("review")) return AgentModel.respond({ approved: true, notes: "reads well" });
  if (text.includes("pick")) return AgentModel.respond("reject");
  if (text.includes("how many")) return n === 0 ? AgentModel.text("lots") : AgentModel.text("42");
  if (text.includes("loop")) return AgentModel.toolCall("lookup_order", { orderId: `loop-${n}` });
  if (text.includes("empty")) return AgentModel.empty;
  if (text.includes("my name is")) return AgentModel.text("Nice to meet you, Ada.");
  if (text.includes("what is my name")) return AgentModel.text(all.includes("Ada") ? "Your name is Ada." : "I don't know.");
  return AgentModel.text(`echo: ${text}`);
};

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

export const DeskLive = Desk.toLayer(
  Desk.of({
    Ask: ({ payload }) => Agent.generate({ prompt: payload.message, session: payload.session, system: "You are a support agent." }),
    // A task that generates, then keeps going: the handler reruns after the turn settles.
    Review: Effect.fn("Desk.Review")(function* ({ payload }) {
      const runs = yield* Testing.count(`review-handler:${payload.article}`);
      const verdict = yield* Agent.generate({ prompt: `review: ${payload.article}`, schema: Verdict });
      return { ...verdict, handlerRuns: runs };
    }),
    // The model answers in text first; the engine must force the `respond` tool.
    ReviewTalkative: ({ payload }) => Agent.generate({ prompt: `talkative review: ${payload.article}`, schema: Verdict }),
    // The model's first structured answer is invalid; the turn must repair it.
    ReviewSloppy: ({ payload }) => Agent.generate({ prompt: `sloppy review: ${payload.article}`, schema: Verdict }),
    // A literal union: answered through `respond`.
    Pick: ({ payload }) => Agent.generate({ prompt: `pick: ${payload.question}`, schema: Schema.Literals(["approve", "reject"]) }),
    // A string-encoded number: answered in text, decoded by the schema (FiniteFromString rejects "lots"; NumberFromString would give NaN).
    Count: ({ payload }) => Agent.generate({ prompt: `how many ${payload.question}`, schema: Schema.FiniteFromString }),
    Counter: ({ payload }) => Effect.map(Testing.count(payload.name), (n) => n - 1),
  }),
  { toolkit: Tools, limits: { maxSteps: 6 } },
).pipe(Layer.provide([AgentModel.scripted(script), ToolsLive]));
