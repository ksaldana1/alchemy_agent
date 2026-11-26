// The support desk's contracts (port of mini-agent-runtime/examples/support-desk; no workflow
// yet, so the ticket text travels in the payload).
import { Agent, Workspace } from "alchemy-agent";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";

export const TriageResult = Schema.Struct({
  category: Schema.Literals(["billing", "bug", "account_access", "feature_request", "other"]),
  urgency: Schema.Literals(["low", "normal", "high", "critical"]),
  needsHuman: Schema.Boolean,
  hasSecret: Schema.Boolean,
});

/** Triage: one structured answer per ticket, then hands the ticket to the responder. */
export const TriageTicket = Rpc.make("TriageTicket", {
  payload: { name: Schema.String, ticket: Schema.String },
  success: TriageResult,
  primaryKey: ({ name }) => name,
});
export const Triage = Agent.make("Triage", [TriageTicket]);

/** Responder: escalates risky tickets without an LLM call; drafts a reply for the rest. */
export const Respond = Rpc.make("Respond", {
  payload: { name: Schema.String, ticket: Schema.String, triage: TriageResult },
  success: Schema.Struct({
    action: Schema.Literals(["escalated", "drafted"]),
    reasons: Schema.Array(Schema.String),
    draft: Schema.NullOr(Schema.String),
  }),
  primaryKey: ({ name }) => name,
});
export const Responder = Agent.make("Responder", [Respond]);

/** Helpdesk: a chat agent with tools, for curl and SSE. */
export const Chat = Rpc.make("Chat", {
  payload: { message: Schema.String, session: Schema.optionalKey(Schema.String) },
  success: Schema.String,
});
export const Helpdesk = Agent.make("Helpdesk", [Chat]);

/** Scribe (M4): an LLM agent that summarizes a knowledge-base doc into its home and shares it. */
export const Summarize = Rpc.make("Summarize", {
  payload: { path: Schema.String, shareWith: Agent.Address },
  success: Workspace.FileRef,
});
export const Scribe = Agent.make("Scribe", [Summarize]);

/** Reader (M4): reads a file as itself. A denial is declared, so the caller gets FsDenied. */
export const ReadFile = Rpc.make("ReadFile", { payload: { path: Schema.String }, success: Schema.String, error: Workspace.FsDenied });
export const Reader = Agent.make("Reader", [ReadFile]);
