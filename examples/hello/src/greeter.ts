import { Agent } from "alchemy-agent";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

class Hiccup extends Data.TaggedError("Hiccup")<{ readonly message: string }> {}

/** Fails with a retryable error on the first attempt, then succeeds. */
const failOnce = Effect.gen(function* () {
  const { attempt } = yield* Agent.AgentContext;
  if (attempt === 1) return yield* new Hiccup({ message: "first attempt always hiccups" });
  return { attempt };
});

export class Greeter extends Agent.make<Greeter>()("Greeter", {
  greet: Agent.task({ name: Schema.String }, ({ name }) =>
    Effect.map(Effect.service(Agent.AgentContext), (ctx) => ({
      greeting: `Hello, ${name}!`,
      by: `${ctx.agent.type}/${ctx.agent.name}`,
    })),
  ),
  failOnce: Agent.task(() => failOnce),
  // Same, but waits 30 s before the retry, so a redeploy can land while it's pending.
  failOnceSlowly: Agent.task({}, { retry: { delay: "30 seconds" } }, () => failOnce),
}) {}
