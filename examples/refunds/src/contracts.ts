// What the refund desk's agents accept and return. No runtime code: scripts and other agents
// import these to call the agents.
import { Agent } from "alchemy-agent";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";

/** A customer's refund request. Amounts are in cents. */
export const RefundRequest = Schema.Struct({
  orderId: Schema.String,
  customer: Schema.String,
  item: Schema.String,
  amount: Schema.Number,
  reason: Schema.String,
});
export type RefundRequest = typeof RefundRequest.Type;

export const Outcome = Schema.Struct({
  orderId: Schema.String,
  status: Schema.Literals(["refunded", "declined"]),
  amount: Schema.Number,
  refundId: Schema.NullOr(Schema.String),
  /** "policy" for refunds under the limit, otherwise the person who decided. */
  decidedBy: Schema.String,
  emailId: Schema.String,
});

// RefundDesk: reads the request with an LLM, asks a person about large refunds, then has the
// refund issued and the customer emailed. Keyed by order: asking twice is the same task.
export const RequestRefund = Rpc.make("RequestRefund", {
  payload: RefundRequest.fields,
  success: Outcome,
  primaryKey: ({ orderId }) => orderId,
});
export const RefundDesk = Agent.make("RefundDesk", [RequestRefund]);

// Payments: talks to the payment gateway.
export const IssueRefund = Rpc.make("IssueRefund", {
  payload: { orderId: Schema.String, amount: Schema.Number },
  success: Schema.Struct({ refundId: Schema.String, amount: Schema.Number }),
});
export const Payments = Agent.make("Payments", [IssueRefund]);

// Mailer: talks to the email provider.
export const SendEmail = Rpc.make("SendEmail", {
  payload: { to: Schema.String, subject: Schema.String, body: Schema.String },
  success: Schema.Struct({ emailId: Schema.String }),
});
export const Mailer = Agent.make("Mailer", [SendEmail]);
