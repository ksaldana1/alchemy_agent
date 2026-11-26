// The support desk's implementations.
import { Agent, AgentModel, Grant, Workspace } from "alchemy-agent";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Helpdesk, Reader, Responder, Scribe, Triage, TriageResult } from "./contracts.ts";

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
/** Delegates to the Triage agent (an LLM agent itself) and waits for its answer (M3). */
const TriageTool = Tool.make("triage_ticket", {
  description: "File the customer's problem as a support ticket. Returns its category and urgency.",
  parameters: Schema.Struct({ ticket: Schema.String }),
  success: TriageResult,
  dependencies: [Agent.ToolCall],
});
const Tools = Toolkit.make(LookupOrder, CheckWarehouse, TriageTool);
const ToolsLive = Tools.toLayer(
  Tools.of({
    lookup_order: ({ orderId }) => Effect.succeed({ orderId, status: "shipped", items: ["blue mug", "tea sampler"] }),
    check_warehouse: ({ product }) => Effect.sleep("15 seconds").pipe(Effect.as({ product, inStock: 42 })),
    // The turn waits (holding no execution) until Triage answers, then this tool returns it.
    triage_ticket: Effect.fn("Helpdesk.triage_ticket")(function* ({ ticket }) {
      const call = yield* Agent.ToolCall;
      const triage = yield* Triage.client;
      return yield* triage("front").TriageTicket({ name: `chat:${call.idempotencyKey}`, ticket }).pipe(Effect.orDie);
    }),
  }),
);

export const HelpdeskLive = Helpdesk.toLayer(
  Helpdesk.of({
    Chat: ({ payload }) =>
      Agent.generate({
        system:
          "You are a support agent. Use the tools to answer questions about orders and stock. When a customer reports " +
          "a problem, file it with triage_ticket and tell them its category and urgency. Be brief.",
        prompt: payload.message,
        session: payload.session,
      }),
  }),
  { toolkit: Tools },
).pipe(Layer.provide([Model, ToolsLive]));

// ---------------------------------------------------------------------------
// The workspace (M4): the knowledge base is in the shared area; only the scribe may read it.
// ---------------------------------------------------------------------------

export const WorkspaceLive = Workspace.layer({ grants: [Grant.read(Scribe.address("kb"), "shared/kb/")], forceDestroy: true });

export const ScribeLive = Scribe.toLayer(
  Scribe.of({
    Summarize: Effect.fn("Scribe.Summarize")(function* ({ payload: { path, shareWith } }) {
      yield* Agent.generate({
        system:
          "You maintain a support knowledge base. Use the tools: read files with read_file and save files with write_file. " +
          "Paths starting with ~/ are your own files.",
        prompt: `Read ${path}, then save a summary of it in at most three sentences to ~/summary.md. Reply "done" once it is saved.`,
      });
      // The model wrote the file through its tools; the handler shares it (only an owner can).
      const summary = yield* Workspace.stat("~/summary.md");
      yield* Workspace.share(summary.path, shareWith, "read");
      return summary;
    }),
  }),
  { toolkit: Workspace.Toolkit },
).pipe(Layer.provide([Model, Workspace.ToolkitLayer]));

export const ReaderLive = Reader.toLayer(Reader.of({ ReadFile: ({ payload }) => Workspace.read(payload.path) }));
