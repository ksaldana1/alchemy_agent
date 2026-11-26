// The support desk (port of mini-agent-runtime/examples/support-desk, M2 subset: no workspace or
// workflow yet, so the ticket text travels in the task input).
import { Agent, AgentModel } from "alchemy-agent";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const MODEL = AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast");

export const TriageResult = Schema.Struct({
  category: Schema.Literals(["billing", "bug", "account_access", "feature_request", "other"]),
  urgency: Schema.Literals(["low", "normal", "high", "critical"]),
  needsHuman: Schema.Boolean,
  hasSecret: Schema.Boolean,
});

/** Responder: escalates risky tickets without an LLM call; drafts a reply for the rest. */
export class Responder extends Agent.make<Responder>()("Responder", {
  model: MODEL,
  instructions:
    "You are a friendly, concise support agent. Draft a reply to the customer in under 120 words. " +
    "Never promise refunds, credits, or timelines. Sign off as 'The Support Team'.",
  tasks: {
    respond: Agent.task({ name: Schema.String, ticket: Schema.String, triage: TriageResult }, ({ ticket, triage }) =>
      Effect.gen(function* () {
        const reasons = [triage.needsHuman && "needs a human", triage.hasSecret && "contains a secret"].filter(
          (r): r is string => typeof r === "string",
        );
        if (reasons.length > 0) return { action: "escalated" as const, reasons, draft: null };
        const draft = yield* Agent.prompt(`Queue: ${triage.category}. Urgency: ${triage.urgency}.\n\nTicket:\n${ticket}`);
        return { action: "drafted" as const, reasons, draft };
      }),
    ),
  },
}) {}

/** Triage: one structured answer per ticket, then hands the ticket to the responder. */
export class Triage extends Agent.make<Triage>()("Triage", {
  model: MODEL,
  uses: [Responder],
  instructions:
    "You triage inbound customer support tickets for a SaaS product. needsHuman: the ticket mentions legal action, " +
    "a chargeback, a security incident, or a customer threatening to cancel. hasSecret: it contains a password, " +
    "API key, or full payment card number.",
  tasks: {
    triage: Agent.task({ name: Schema.String, ticket: Schema.String }, ({ name, ticket }) =>
      Effect.gen(function* () {
        const triage = yield* Agent.prompt(ticket, { output: TriageResult });
        const { taskId } = yield* Agent.AgentContext;
        // A derived task id: if this handler runs again, the send is a no-op.
        yield* Responder.named("desk").send.respond({ name, ticket, triage }, { taskId: `${taskId}:respond` });
        return triage;
      }),
    ),
  },
}) {}

const LookupOrder = Tool.make("lookup_order", {
  description: "Look up an order by its id. Returns its status and items.",
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ orderId: Schema.String, status: Schema.String, items: Schema.Array(Schema.String) }),
});
const CheckWarehouse = Tool.make("check_warehouse", {
  description: "Check warehouse stock for a product. Slow: takes about 15 seconds.",
  parameters: Schema.Struct({ product: Schema.String }),
  success: Schema.Struct({ product: Schema.String, inStock: Schema.Number }),
});

/** Helpdesk: a chat agent with tools, for curl/SSE. */
export class Helpdesk extends Agent.make<Helpdesk>()("Helpdesk", {
  model: MODEL,
  instructions: "You are a support agent. Use the tools to answer questions about orders and stock. Be brief.",
  tools: Agent.tools(Toolkit.make(LookupOrder, CheckWarehouse), {
    lookup_order: ({ orderId }) => Effect.succeed({ orderId, status: "shipped", items: ["blue mug", "tea sampler"] }),
    check_warehouse: ({ product }) => Effect.sleep("15 seconds").pipe(Effect.as({ product, inStock: 42 })),
  }),
  tasks: {},
}) {}
