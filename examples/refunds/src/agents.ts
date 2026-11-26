// How the refund desk's agents do their work. RefundDesk needs a model: `RefundDeskLive` provides
// Workers AI; the demo provides a scripted one instead.
import { Agent, AgentModel, Human } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Mailer, Payments, RefundDesk, type RefundRequest } from "./contracts.ts";
import { EmailProvider, PaymentGateway, SimulatedEmail, SimulatedGateway } from "./providers.ts";

/** Refunds above this (in cents) need a person's approval. A rule in code, not a judgment call for the model. */
export const APPROVAL_LIMIT = 200_00;

export const Assessment = Schema.Struct({
  eligible: Schema.Boolean,
  /** One sentence for the person approving, if one is needed. */
  summary: Schema.String,
  /** The email to the customer, if the refund goes through. */
  acceptReply: Schema.String,
  /** The email to the customer, if it doesn't. */
  declineReply: Schema.String,
});

export const dollars = (cents: number) => `$${(cents / 100).toFixed(2)}`;

export const assessmentPrompt = (r: RefundRequest) =>
  `Order ${r.orderId}: ${r.customer} bought "${r.item}" for ${dollars(r.amount)} and asks for a refund.\nTheir reason: ${r.reason}`;

export const RefundDeskAgent = RefundDesk.toLayer(
  Effect.gen(function* () {
    const payments = (yield* Payments.client)("acme-pay");
    const mailer = (yield* Mailer.client)("support");

    return RefundDesk.of({
      // This handler waits three times (the model, maybe a person, then each call) and runs again
      // from the top after each wait. Every step before the current one returns its recorded
      // result, so the model is asked once and the refund is issued once.
      RequestRefund: Effect.fn("RefundDesk.RequestRefund")(function* ({ payload: request }) {
        const assessment = yield* Agent.generate({
          system:
            "You handle refund requests for an online store. Refunds are allowed within 30 days for items that are " +
            "damaged, defective, wrong, or not delivered. Decide if the request is eligible, summarize it in one " +
            "sentence for a manager, and write two short, friendly emails to the customer: one if it's refunded, " +
            "one if it isn't.",
          prompt: assessmentPrompt(request),
          schema: Assessment,
        });

        let decidedBy = "policy";
        let approved = assessment.eligible;
        if (approved && request.amount > APPROVAL_LIMIT) {
          // Waits for a person (GET /requests, POST /requests/:id/respond). It holds no execution
          // while it waits, so a deploy or an eviction in the meantime changes nothing.
          const decision = yield* Human.request(`refund ${request.orderId}`, {
            schema: Human.Approval,
            prompt: `Refund ${dollars(request.amount)} to ${request.customer} for "${request.item}"? ${assessment.summary}`,
            timeout: "3 days",
          }).pipe(
            Effect.catchTags({
              RequestExpired: () => Effect.succeed({ approved: false, notes: "nobody answered in time" }),
              RequestCanceled: (e) => Effect.succeed({ approved: false, notes: e.reason }),
            }),
          );
          decidedBy = "manager";
          approved = decision.approved;
        }

        const refund = approved ? yield* payments.IssueRefund({ orderId: request.orderId, amount: request.amount }) : null;
        const { emailId } = yield* mailer.SendEmail({
          to: request.customer,
          subject: `Your refund request for order ${request.orderId}`,
          body: refund ? assessment.acceptReply : assessment.declineReply,
        });
        return {
          orderId: request.orderId,
          status: refund ? "refunded" : "declined",
          amount: refund?.amount ?? 0,
          refundId: refund?.refundId ?? null,
          decidedBy,
          emailId,
        } as const;
      }),
    });
  }),
);

export const PaymentsAgent = Payments.toLayer(
  Effect.gen(function* () {
    const gateway = yield* PaymentGateway;
    return Payments.of({
      // The task id is the same on every delivery of this task, so it's the idempotency key: if
      // this agent dies after the gateway moved the money, the redelivery gets the same refund back.
      IssueRefund: ({ payload, taskId }) => gateway.refund({ idempotencyKey: taskId, ...payload }),
    });
  }),
);

export const MailerAgent = Mailer.toLayer(
  Effect.gen(function* () {
    const email = yield* EmailProvider;
    return Mailer.of({
      SendEmail: Effect.fn("Mailer.SendEmail")(function* ({ payload, taskId }) {
        return { emailId: yield* email.send({ idempotencyKey: taskId, ...payload }) };
      }),
    });
  }),
);

export const RefundDeskLive = RefundDeskAgent.pipe(Layer.provide(AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast")));
export const PaymentsLive = PaymentsAgent.pipe(Layer.provide(SimulatedGateway));
export const MailerLive = MailerAgent.pipe(Layer.provide(SimulatedEmail));
