// The refund desk for the demo: the same agents, a scripted model (no LLM bill, same answers every
// run), and planned failures. Chaos lives here, never in the app code.
import { AgentModel, Platform } from "alchemy-agent";
import * as Testing from "alchemy-agent/testing";
import type * as Prompt from "effect/ai/Prompt";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { MailerAgent, PaymentsAgent, RefundDeskAgent } from "../src/agents.ts";
import { Mailer } from "../src/contracts.ts";
import { PaymentGateway, SimulatedEmail, SimulatedGateway } from "../src/providers.ts";

/** Answers like a model would: anything "changed my mind" isn't eligible; the rest is. */
const script = (prompt: Prompt.Prompt): AgentModel.ScriptedResponse => {
  const user = prompt.content.filter((m) => m.role === "user").at(-1);
  const text = user === undefined ? "" : typeof user.content === "string" ? user.content : user.content.map((p) => ("text" in p ? p.text : "")).join("");
  const item = text.match(/bought "([^"]+)"/)?.[1] ?? "your item";
  const eligible = !/changed my mind/i.test(text);
  return AgentModel.respond({
    eligible,
    summary: eligible ? `The ${item} arrived damaged; within the 30-day window.` : "Change of mind after 30 days; not covered by the policy.",
    acceptReply: `Sorry about the ${item}! Your refund is on its way and should arrive in 5 to 10 days.`,
    declineReply: `Thanks for reaching out about the ${item}. It's outside our 30-day return window, so we can't refund it.`,
  });
};

/**
 * The worst moment to die: the gateway has moved the money, and the Payments agent dies before it
 * records the answer (an eviction, a CPU limit, a deploy). The first time only.
 */
export const LostResponseGateway = Layer.effect(
  PaymentGateway,
  Effect.gen(function* () {
    const gateway = yield* PaymentGateway;
    return {
      refund: (request) =>
        Effect.gen(function* () {
          const refund = yield* gateway.refund(request);
          if ((yield* Testing.count("lost-response")) === 1) yield* Testing.crashExecution;
          return refund;
        }),
    };
  }),
).pipe(Layer.provide(SimulatedGateway));

export default Platform.make("RefundsDemoApi", {
  main: import.meta.url,
  agents: [
    RefundDeskAgent.pipe(Layer.provide(AgentModel.scripted(script))),
    PaymentsAgent.pipe(Layer.provide(LostResponseGateway)),
    MailerAgent.pipe(Layer.provide(SimulatedEmail)),
  ],
  workspace: { forceDestroy: true },
  // The mailer also dies once, as its first email starts.
  provide: Testing.chaos([Testing.crashOnce(Mailer, "SendEmail")]),
});
