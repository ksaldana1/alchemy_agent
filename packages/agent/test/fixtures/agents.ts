// Test agents. Each task exercises one inbox behavior.
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Agent from "../../src/Agent.ts";

class Flake extends Data.TaggedError("Flake")<{ readonly message: string }> {}

let order = 0;

export class Echo extends Agent.make<Echo>()("Echo", {
  tasks: {
    echo: Agent.task({ input: Schema.Struct({ text: Schema.String }) }, ({ text }) =>
      Effect.succeed({ echoed: text.toUpperCase() }),
    ),
    whoami: Agent.task({ input: Schema.Struct({}) }, () =>
      Effect.map(Effect.service(Agent.AgentContext), (ctx) => ({
        type: ctx.agent.type,
        name: ctx.agent.name,
        taskId: ctx.taskId,
        attempt: ctx.attempt,
      })),
    ),
    // Execution order within one isolate: proves FIFO.
    order: Agent.task({ input: Schema.Struct({ label: Schema.String }) }, ({ label }) =>
      Effect.sync(() => ({ label, n: ++order })),
    ),
    // A declared output schema: Date in the handler, ISO string on the wire.
    stamp: Agent.task(
      {
        input: Schema.Struct({ epochMs: Schema.Number }),
        output: Schema.Struct({ at: Schema.DateFromString }),
      },
      ({ epochMs }) => Effect.succeed({ at: new Date(epochMs) }),
    ),
  },
}) {}

export class Flaky extends Agent.make<Flaky>()("Flaky", {
  retry: { maxAttempts: 3, delay: "200 millis" },
  tasks: {
    // Fails with a retryable tagged error while attempt <= failTimes.
    flaky: Agent.task({ input: Schema.Struct({ failTimes: Schema.Number }) }, ({ failTimes }) =>
      Effect.gen(function* () {
        const { attempt } = yield* Agent.AgentContext;
        if (attempt <= failTimes) return yield* new Flake({ message: `flake #${attempt}` });
        return { attempt };
      }),
    ),
    permanent: Agent.task({ input: Schema.Struct({}) }, () =>
      Effect.fail(new Agent.PermanentError({ message: "nope" })),
    ),
    die: Agent.task({ input: Schema.Struct({}) }, () => Effect.die(new Error("boom"))),
    // Ends the execution abruptly while attempt <= times (like an eviction mid-task).
    crash: Agent.task({ input: Schema.Struct({ times: Schema.Number }) }, ({ times }) =>
      Effect.gen(function* () {
        const { attempt } = yield* Agent.AgentContext;
        if (attempt <= times) return yield* Agent.Testing.crashExecution;
        return { attempt };
      }),
    ),
    // Takes a while, so a test can observe it `running` / redeploy around it.
    slow: Agent.task(
      { input: Schema.Struct({ ms: Schema.Number }), retry: { maxAttempts: 1 } },
      ({ ms }) => Effect.sleep(`${ms} millis`).pipe(Effect.as({ slept: ms })),
    ),
  },
}) {}

// Agent B sends to agent A through `uses`.
export class Relay extends Agent.make<Relay>()("Relay", {
  uses: [Echo],
  tasks: {
    forward: Agent.task({ input: Schema.Struct({ text: Schema.String }) }, ({ text }) =>
      Effect.gen(function* () {
        const ctx = yield* Agent.AgentContext;
        const echo = yield* Agent.use(Echo);
        const handle = yield* echo
          .getByName(`from-${ctx.agent.name}`)
          .send("echo", { text }, { taskId: `${ctx.taskId}:echo` });
        return { taskId: handle.taskId };
      }),
    ),
  },
}) {}
