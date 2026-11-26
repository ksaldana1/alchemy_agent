// The support desk's implementations.
import { Agent, AgentModel } from "alchemy-agent";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Helpdesk, Responder, Triage, TriageResult } from "./contracts.ts";

const Model = AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast");

export const TriageLive = Triage.toLayer(
  Effect.gen(function* () {
    const responders = yield* Responder.client;
    return Triage.of({
      TriageTicket: Effect.fn("Triage.TriageTicket")(function* ({ payload: { name, ticket } }) {
        const triage = yield* Agent.generate({
          system:
            "You triage inbound customer support tickets for a SaaS product. needsHuman: the ticket mentions legal action, " +
            "a chargeback, a security incident, or a customer threatening to cancel. hasSecret: it contains a password, " +
            "API key, or full payment card number.",
          prompt: ticket,
          schema: TriageResult,
        });
        // Keyed by ticket name: if this handler runs again, the send is a no-op.
        yield* responders("desk").Respond({ name, ticket, triage }, { discard: true });
        return triage;
      }),
    });
  }),
).pipe(Layer.provide(Model));

export const ResponderLive = Responder.toLayer(
  Responder.of({
    Respond: Effect.fn("Responder.Respond")(function* ({ payload: { ticket, triage } }) {
      const reasons = [triage.needsHuman && "needs a human", triage.hasSecret && "contains a secret"].filter(
        (r): r is string => typeof r === "string",
      );
      if (reasons.length > 0) return { action: "escalated" as const, reasons, draft: null };
      const draft = yield* Agent.generate({
        system:
          "You are a friendly, concise support agent. Draft a reply to the customer in under 120 words. " +
          "Never promise refunds, credits, or timelines. Sign off as 'The Support Team'.",
        prompt: `Queue: ${triage.category}. Urgency: ${triage.urgency}.\n\nTicket:\n${ticket}`,
      });
      return { action: "drafted" as const, reasons, draft };
    }),
  }),
).pipe(Layer.provide(Model));

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
const Tools = Toolkit.make(LookupOrder, CheckWarehouse);
const ToolsLive = Tools.toLayer(
  Tools.of({
    lookup_order: ({ orderId }) => Effect.succeed({ orderId, status: "shipped", items: ["blue mug", "tea sampler"] }),
    check_warehouse: ({ product }) => Effect.sleep("15 seconds").pipe(Effect.as({ product, inStock: 42 })),
  }),
);

export const HelpdeskLive = Helpdesk.toLayer(
  Helpdesk.of({
    Chat: ({ payload }) =>
      Agent.generate({
        system: "You are a support agent. Use the tools to answer questions about orders and stock. Be brief.",
        prompt: payload.message,
        session: payload.session,
      }),
  }),
  { toolkit: Tools },
).pipe(Layer.provide([Model, ToolsLive]));
