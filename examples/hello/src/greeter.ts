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
  retry: { maxAttempts: 3, delay: "1 second" },
  tasks: {
    greet: Agent.task({ input: Schema.Struct({ name: Schema.String }) }, ({ name }) =>
      Effect.gen(function* () {
        const ctx = yield* Agent.AgentContext;
        return { greeting: `Hello, ${name}!`, by: `${ctx.agent.type}/${ctx.agent.name}` };
      }),
    ),
    failOnce: Agent.task({ input: Schema.Struct({}) }, () => failOnce),
    // Same, but waits 30 s before the retry, so a redeploy can land while it's pending.
    failOnceSlowly: Agent.task(
      { input: Schema.Struct({}), retry: { delay: "30 seconds" } },
      () => failOnce,
    ),
  },
}) {}
