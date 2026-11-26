import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Agent from "./agent.ts";

// Spike 0.2 agent A: the target shape from docs/spike/0.2-agent-typing.md.
export default class Echo extends Agent.make<Echo>()("Echo", {
  tasks: {
    echo: Agent.task({ input: Schema.Struct({ text: Schema.String }) }, ({ text }) =>
      Effect.succeed({ echoed: text.toUpperCase() }),
    ),
    count: Agent.task({ input: Schema.Struct({ n: Schema.Number }) }, ({ n }) =>
      Effect.gen(function* () {
        const ctx = yield* Agent.AgentContext;
        return { n, by: ctx.agent.name };
      }),
    ),
  },
}) {}
