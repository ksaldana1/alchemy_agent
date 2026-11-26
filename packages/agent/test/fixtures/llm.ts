// LLM test agents on a scripted model: deterministic, no network.
import * as Prompt from "effect/ai/Prompt";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Agent from "../../src/Agent.ts";
import * as AgentModel from "../../src/AgentModel.ts";
import * as Testing from "../../src/Testing.ts";

const LookupOrder = Tool.make("lookup_order", {
  description: "Look up an order by id.",
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ orderId: Schema.String, status: Schema.String }),
});
const Crashy = Tool.make("crashy", {
  description: "Crashes the execution the first time it runs.",
  success: Schema.Struct({ runs: Schema.Number }),
});

/** The last user message, and how many assistant messages answered it so far. */
const latest = (prompt: Prompt.Prompt) => {
  const msgs = prompt.content;
  let i = msgs.length - 1;
  while (i >= 0 && msgs[i]!.role !== "user") i--;
  const user = msgs[i];
  const text =
    user === undefined ? "" : typeof user.content === "string" ? user.content : user.content.map((p) => ("text" in p ? p.text : "")).join("");
  return { text, n: msgs.slice(i + 1).filter((m) => m.role === "assistant").length, all: JSON.stringify(msgs) };
};

/** Replies chosen by keyword in the latest user message; `n` = how many replies so far. */
const script = (prompt: Prompt.Prompt): AgentModel.ScriptedResponse => {
  const { text, n, all } = latest(prompt);
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
  if (text.includes("loop")) return AgentModel.toolCall("lookup_order", { orderId: `loop-${n}` });
  if (text.includes("empty")) return AgentModel.empty;
  if (text.includes("my name is")) return AgentModel.text("Nice to meet you, Ada.");
  if (text.includes("what is my name")) return AgentModel.text(all.includes("Ada") ? "Your name is Ada." : "I don't know.");
  if (text.includes("slow")) return AgentModel.text(`echo: ${text}`);
  return AgentModel.text(`echo: ${text}`);
};

const Verdict = Schema.Struct({ approved: Schema.Boolean, notes: Schema.String });

export class Desk extends Agent.make<Desk>()("Desk", {
  model: AgentModel.scripted(script),
  instructions: "You are a support agent.",
  limits: { maxSteps: 6 },
  tools: Agent.tools(Toolkit.make(LookupOrder, Crashy), {
    lookup_order: ({ orderId }) =>
      Effect.gen(function* () {
        yield* Testing.count(`lookup:${orderId}`);
        return { orderId, status: "shipped" };
      }),
    crashy: () =>
      Effect.gen(function* () {
        const runs = yield* Testing.count("crashy");
        if (runs === 1) return yield* Testing.crashExecution;
        return { runs };
      }),
  }),
  tasks: {
    // A task that prompts, then keeps going: the handler reruns after the turn settles.
    review: Agent.task({ article: Schema.String }, ({ article }) =>
      Effect.gen(function* () {
        const runs = yield* Testing.count(`review-handler:${article}`);
        const verdict = yield* Agent.prompt(`review: ${article}`, { output: Verdict });
        return { ...verdict, handlerRuns: runs };
      }),
    ),
    // The model answers in text first; the engine must force the `respond` tool.
    reviewTalkative: Agent.task({ article: Schema.String }, ({ article }) =>
      Agent.prompt(`talkative review: ${article}`, { output: Verdict }),
    ),
    // The model's first structured answer is invalid; the turn must repair it.
    reviewSloppy: Agent.task({ article: Schema.String }, ({ article }) =>
      Agent.prompt(`sloppy review: ${article}`, { output: Verdict }),
    ),
    // How many times tools ran, read from the durable counters.
    counter: Agent.task({ name: Schema.String }, ({ name }) =>
      Effect.map(Testing.count(name), (n) => ({ n: n - 1 })),
    ),
  },
}) {}
