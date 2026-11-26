// The outside world: a payment gateway and an email provider. In a real app these call Stripe
// and Postmark (or similar). Here they are simulated, and keep their records in the calling
// agent's workspace home so the demo can show exactly what reached them.
//
// Like Stripe's API, the gateway takes an idempotency key: a request with a key it has seen
// before returns the refund it already made instead of making another.
import { Workspace, type Agent } from "alchemy-agent";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

export interface Refund {
  readonly refundId: string;
  readonly amount: number;
}

export class PaymentGateway extends Context.Service<
  PaymentGateway,
  {
    readonly refund: (request: { readonly idempotencyKey: string; readonly orderId: string; readonly amount: number }) => Effect.Effect<Refund, never, Agent.AgentContext>;
  }
>()("refunds/PaymentGateway") {}

export class EmailProvider extends Context.Service<
  EmailProvider,
  {
    readonly send: (email: { readonly idempotencyKey: string; readonly to: string; readonly subject: string; readonly body: string }) => Effect.Effect<string, never, Agent.AgentContext>;
  }
>()("refunds/EmailProvider") {}

/** File names can't hold every character a task id can. */
const fileKey = (key: string) => encodeURIComponent(key);

/** The simulated gateway. Its ledger: `~/gateway/requests/` (every request) and `~/gateway/refunds/` (money moved). */
export const SimulatedGateway = Layer.succeed(PaymentGateway, {
  refund: Effect.fn("SimulatedGateway.refund")(
    function* ({ idempotencyKey, orderId, amount }) {
      const key = fileKey(idempotencyKey);
      yield* Workspace.write(`~/gateway/requests/${key}--${crypto.randomUUID()}.json`, JSON.stringify({ idempotencyKey, orderId, amount }));
      const existing = yield* Workspace.read(`~/gateway/refunds/${key}.json`).pipe(Effect.catchTag("FileNotFound", () => Effect.succeed(null)));
      if (existing !== null) return JSON.parse(existing) as Refund;
      const refund: Refund = { refundId: `re_${crypto.randomUUID().slice(0, 8)}`, amount };
      yield* Workspace.write(`~/gateway/refunds/${key}.json`, JSON.stringify({ ...refund, orderId }));
      return refund;
    },
    Effect.orDie,
  ),
});

/** The simulated email provider: one file per email in `~/sent/`, named by its idempotency key. */
export const SimulatedEmail = Layer.succeed(EmailProvider, {
  send: Effect.fn("SimulatedEmail.send")(
    function* ({ idempotencyKey, to, subject, body }) {
      const emailId = `em_${fileKey(idempotencyKey)}`;
      yield* Workspace.write(`~/sent/${emailId}.json`, JSON.stringify({ to, subject, body }));
      return emailId;
    },
    Effect.orDie,
  ),
});
