import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Agent from "./agent.ts";
import Echo from "./echo.ts";

// Spike 0.2 agent B: holds a typed reference to Echo, and exercises errors and output schemas.
export default class Relay extends Agent.make<Relay>()("Relay", {
  uses: [Echo],
  tasks: {
    // Calls agent A (Echo) from inside agent B's DO.
    relay: Agent.task({ input: Schema.Struct({ text: Schema.String }) }, ({ text }) =>
      Effect.gen(function* () {
        const ctx = yield* Agent.AgentContext;
        const echo = yield* Agent.use(Echo);
        const a = echo.getByName(`relay-${ctx.agent.name}`);
        const handle = yield* a.send(`${ctx.taskId}:echo`, "echo", { text }).pipe(Effect.orDie);
        const record = yield* a.result(handle);
        return { via: ctx.agent.name, record };
      }),
    ),
    // A handler that fails with a tagged error the agent marks as permanent.
    fail: Agent.task({ input: Schema.Struct({ reason: Schema.String }) }, ({ reason }) =>
      Effect.fail(new Agent.PermanentError({ message: reason })),
    ),
    // A declared output schema whose Type (Date) differs from its Encoded form (string).
    stamp: Agent.task(
      {
        input: Schema.Struct({ epochMs: Schema.Number }),
        output: Schema.Struct({ at: Schema.DateFromString }),
      },
      ({ epochMs }) => Effect.succeed({ at: new Date(epochMs) }),
    ),
  },
}) {}
