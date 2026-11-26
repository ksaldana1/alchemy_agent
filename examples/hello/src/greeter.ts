// The Greeter: a contract (Rpcs + Agent.make) and its implementation (a Layer).
import { Agent } from "alchemy-agent";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";

export const Greet = Rpc.make("Greet", {
  payload: { name: Schema.String },
  success: Schema.Struct({ greeting: Schema.String, by: Schema.String }),
});
/** Keyed: welcoming the same key again is the same task (a no-op once it settled). */
export const Welcome = Rpc.make("Welcome", {
  payload: { key: Schema.String, name: Schema.String },
  success: Schema.Struct({ greeting: Schema.String }),
  primaryKey: ({ key }) => key,
});
export const FailOnce = Rpc.make("FailOnce", { payload: { key: Schema.String }, success: Schema.Struct({ attempt: Schema.Number }), primaryKey: ({ key }) => key });
/** Same, but its retry waits 30 s, so a redeploy can land while it's pending. */
export const FailOnceSlowly = Rpc.make("FailOnceSlowly", {
  payload: { key: Schema.String },
  success: Schema.Struct({ attempt: Schema.Number }),
  primaryKey: ({ key }) => key,
}).annotate(Agent.Retry, { delay: "30 seconds" });
export const Greeter = Agent.make("Greeter", [Greet, Welcome, FailOnce, FailOnceSlowly]);

class Hiccup extends Data.TaggedError("Hiccup")<{ readonly message: string }> {}
const failOnce = (attempt: number) =>
  attempt === 1 ? Effect.fail(new Hiccup({ message: "first attempt always hiccups" })) : Effect.succeed({ attempt });

export const GreeterLive = Greeter.toLayer(
  Greeter.of({
    Greet: ({ payload, agent }) => Effect.succeed({ greeting: `Hello, ${payload.name}!`, by: `${agent.type}/${agent.name}` }),
    Welcome: ({ payload }) => Effect.succeed({ greeting: `Welcome, ${payload.name}!` }),
    FailOnce: ({ attempt }) => failOnce(attempt),
    FailOnceSlowly: ({ attempt }) => failOnce(attempt),
  }),
  { retry: { maxAttempts: 3, delay: "1 second" } },
);
