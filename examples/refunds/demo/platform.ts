// The refund desk for the demo: the same agents, a scripted model (no LLM bill, the same answers
// every run), crashes on demand, and a live page at `/`. Chaos lives here, never in the app code.
import * as Cloudflare from "@alchemy.run/cloudflare";
import { AgentModel, Api, Platform, Workspace } from "alchemy-agent";
import * as Testing from "alchemy-agent/testing";
import type * as Prompt from "effect/ai/Prompt";
import * as Effect from "effect/Effect";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import { MailerAgent, PaymentsAgent, RefundDeskAgent } from "../src/agents.ts";
import { PaymentGateway, SimulatedEmail, SimulatedGateway } from "../src/providers.ts";
import { page } from "./page.ts";

/** Answers like a model would: "changed my mind" isn't eligible; anything else is. */
const script = (prompt: Prompt.Prompt): AgentModel.ScriptedResponse => {
  const user = prompt.content.filter((m) => m.role === "user").at(-1);
  const text = user === undefined ? "" : typeof user.content === "string" ? user.content : user.content.map((p) => ("text" in p ? p.text : "")).join("");
  const item = text.match(/bought "([^"]+)"/)?.[1] ?? "your item";
  const eligible = !/changed my mind/i.test(text);
  return AgentModel.respond({
    eligible,
    summary: eligible ? `Customer reports a problem with the ${item}; within the 30-day window.` : "Change of mind after 30 days; not covered by the policy.",
    acceptReply: `Sorry about the ${item}! Your refund is on its way and should arrive in 5 to 10 days.`,
    declineReply: `Thanks for reaching out about the ${item}. It's outside our 30-day return window, so we can't refund it.`,
  });
};

/** Where a crash is armed for an order: a file in the Payments agent's home (the page writes it). */
export const crashMarker = (orderId: string) => `agents/Payments/acme-pay/chaos/${orderId}`;

/**
 * The worst moment to die: the gateway has moved the money, and the Payments agent dies before it
 * records the answer (an eviction, a CPU limit, a deploy). Only for orders with a crash armed, and
 * only on the first delivery.
 */
const CrashAfterRefund = Layer.effect(
  PaymentGateway,
  Effect.gen(function* () {
    const gateway = yield* PaymentGateway;
    return {
      refund: (request) =>
        Effect.gen(function* () {
          const refund = yield* gateway.refund(request);
          const armed = yield* Workspace.stat(`~/chaos/${request.orderId}`).pipe(
            Effect.as(true),
            Effect.catchTag("FileNotFound", () => Effect.succeed(false)),
            Effect.orDie,
          );
          if (armed && (yield* Testing.count(`crash:${request.idempotencyKey}`)) === 1) yield* Testing.crashExecution;
          return refund;
        }),
    };
  }),
).pipe(Layer.provide(SimulatedGateway));

export default Cloudflare.Worker(
  "RefundsDemoApi",
  { main: import.meta.url },
  Effect.gen(function* () {
    const api = yield* Api.routes();
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.method === "GET" && new URL(request.url, "http://local").pathname === "/") return HttpServerResponse.html(page);
        return (yield* api.handle) ?? HttpServerResponse.text("Not found", { status: 404 });
      }),
    };
  }).pipe(
    Effect.provide(
      Platform.layer({
        agents: [
          RefundDeskAgent.pipe(Layer.provide(AgentModel.scripted(script))),
          PaymentsAgent.pipe(Layer.provide(CrashAfterRefund)),
          MailerAgent.pipe(Layer.provide(SimulatedEmail)),
        ],
        workspace: { forceDestroy: true },
      }),
    ),
  ),
);
