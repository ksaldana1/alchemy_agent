// The refund desk demo: four refund requests while things go wrong, narrated, then the providers'
// ledgers. Every claim it prints is also checked.
//
//   bun run refunds:demo          local (workerd), no LLM, no Cloudflare resources left behind
//   bun run refunds:demo:cloud    the same on a real test_$USER stage, destroyed afterwards
//
// What goes wrong (demo/platform.ts):
//   - The payment gateway refunds the first order, and the Payments agent dies before it records
//     the answer. The task is delivered again.
//   - The same request arrives twice.
//
// `bun run refunds:ui` runs the same stack with a page to click through it yourself.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { Api, Platform } from "alchemy-agent";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { dollars } from "../src/agents.ts";
import { RefundDesk, type RefundRequest } from "../src/contracts.ts";
import { crashMarker } from "../demo/platform.ts";
import Stack from "../demo/stack.ts";

process.env[Api.TOKEN_ENV] ??= "test-token";
const token = process.env[Api.TOKEN_ENV]!;

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({ providers: Cloudflare.providers(), state: Cloudflare.state() });
const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

const run = crypto.randomUUID().slice(0, 4);
const order = (n: number, item: string, amount: number, reason: string): RefundRequest => ({
  orderId: `${run}-${n}`,
  customer: `customer${n}@example.com`,
  item,
  amount,
  reason,
});
const KETTLE = order(1042, "Pour-over kettle", 34_00, "It arrived with a cracked handle.");
const ESPRESSO = order(1043, "Espresso machine", 649_00, "The pump stopped working after a week.");
const THROW = order(1044, "Linen throw", 89_00, "I changed my mind, it's been about two months.");

const say = (line = "") => console.log(line);
const step = (line: string) => console.log(`   ${line}`);

test(
  "each refund is issued once, and each email sent once, while things go wrong",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const api = (path: string) =>
      Effect.promise(async () => {
        for (let i = 0; ; i++) {
          const res = await fetch(`${url}${path}`, { headers: { authorization: `Bearer ${token}` } });
          if (res.ok || i === 60) return (await res.json()) as any;
          await new Promise((r) => setTimeout(r, 1000)); // a fresh workers.dev URL can 404 for a while
        }
      });
    yield* api("/agents");

    yield* Effect.gen(function* () {
      const platform = yield* Platform.Client;
      const desk = (yield* RefundDesk.client)("acme");
      const tasks = (type: string, name: string) => api(`/agents/${type}/${name}/tasks`) as Effect.Effect<ReadonlyArray<any>>;

      say();
      say(`1. Order ${KETTLE.orderId}: ${KETTLE.item}, ${dollars(KETTLE.amount)}. "${KETTLE.reason}"`);
      step("Under the approval limit, so policy approves it.");
      step("Planted failure: the gateway refunds it, then Payments dies before recording the answer.");
      yield* platform.files.write(crashMarker(KETTLE.orderId), "armed");
      const kettle = yield* desk.RequestRefund(KETTLE);
      step(`→ ${kettle.status} ${dollars(kettle.amount)} (${kettle.refundId}), and the customer was emailed.`);
      expect(kettle).toMatchObject({ status: "refunded", amount: KETTLE.amount, decidedBy: "policy" });

      say();
      say(`2. Order ${ESPRESSO.orderId}: ${ESPRESSO.item}, ${dollars(ESPRESSO.amount)}. "${ESPRESSO.reason}"`);
      step("Over the approval limit, so the agent asks a manager and waits.");
      yield* desk.RequestRefund(ESPRESSO, { discard: true });
      const [request] = yield* platform.requests.stream.pipe(Stream.take(1), Stream.runCollect, Effect.timeout("2 minutes"));
      const waiting = yield* api(`/agents/RefundDesk/acme/tasks/${encodeURIComponent(`RequestRefund:${ESPRESSO.orderId}`)}`);
      step(`Manager's queue: "${request!.prompt}"`);
      step(`The agent's task is "${waiting.status}". No code runs while it waits: it could be days, through any number of deploys.`);
      expect(waiting.status).toBe("waiting");
      yield* request!.respond({ approved: true }, { by: "manager@acme.example" });
      step("The manager approves.");
      const espresso = yield* desk.RequestRefund(ESPRESSO);
      step(`→ ${espresso.status} ${dollars(espresso.amount)} (${espresso.refundId}), approved by the ${espresso.decidedBy}`);
      expect(espresso).toMatchObject({ status: "refunded", amount: ESPRESSO.amount, decidedBy: "manager" });

      say();
      say(`3. Order ${THROW.orderId}: ${THROW.item}, ${dollars(THROW.amount)}. "${THROW.reason}"`);
      const linen = yield* desk.RequestRefund(THROW);
      step(`→ ${linen.status}: the model found it outside the policy. No refund; a polite email instead.`);
      expect(linen).toMatchObject({ status: "declined", refundId: null });

      say();
      say(`4. Order ${KETTLE.orderId} again (a double-click, or a webhook delivered twice).`);
      const again = yield* desk.RequestRefund(KETTLE);
      step(`→ the same task, the same answer: ${again.refundId}`);
      expect(again).toEqual(kettle);

      // The ledgers: what actually reached the outside world.
      const gatewayRequests = (yield* api("/workspace/list/agents/Payments/acme-pay/gateway/requests/")) as ReadonlyArray<string>;
      const refunds = (yield* api("/workspace/list/agents/Payments/acme-pay/gateway/refunds/")) as ReadonlyArray<string>;
      const emails = (yield* api("/workspace/list/agents/Mailer/support/sent/")) as ReadonlyArray<string>;
      const payments = yield* tasks("Payments", "acme-pay");
      const mails = yield* tasks("Mailer", "support");

      say();
      say("What reached the outside world:");
      step(`Payment gateway: ${gatewayRequests.length} requests, ${refunds.length} refunds issued.`);
      step(`  The first refund's task was delivered ${payments[0]?.attempts} times, with the same idempotency key, so the gateway returned the refund it had already made.`);
      step(`Email provider: ${emails.length} emails sent, one per order.`);
      say();

      expect(refunds.length).toBe(2);
      expect(gatewayRequests.length).toBe(3);
      expect(payments.map((t) => t.attempts)).toEqual([2, 1]);
      expect(emails.length).toBe(3);
      expect(mails.length).toBe(3);
    }).pipe(Effect.provide(Platform.connect({ url: url!, token })), Effect.scoped);
  }),
  { timeout: 300_000 },
);
