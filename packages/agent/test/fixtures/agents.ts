// Test agents. Each task exercises one inbox behavior.
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Agent from "../../src/Agent.ts";
import * as Testing from "../../src/Testing.ts";

class Flake extends Data.TaggedError("Flake")<{ readonly message: string }> {}

let order = 0;

export class Echo extends Agent.make<Echo>()("Echo", {
  echo: Agent.task({ text: Schema.String }, ({ text }) => Effect.succeed({ echoed: text.toUpperCase() })),
  whoami: Agent.task(() =>
    Effect.map(Effect.service(Agent.AgentContext), (ctx) => ({
      type: ctx.agent.type,
      name: ctx.agent.name,
      taskId: ctx.taskId,
      attempt: ctx.attempt,
    })),
  ),
  // Execution order within one isolate: proves FIFO.
  order: Agent.task({ label: Schema.String }, ({ label }) => Effect.sync(() => ({ label, n: ++order }))),
  // A declared output schema: Date in the handler, ISO string on the wire.
  stamp: Agent.task({ epochMs: Schema.Number }, { output: Schema.Struct({ at: Schema.DateFromString }) }, ({ epochMs }) =>
    Effect.succeed({ at: new Date(epochMs) }),
  ),
  // Takes a while, so a caller's wait has to span the handler.
  slow: Agent.task({ ms: Schema.Number }, ({ ms }) => Effect.sleep(`${ms} millis`).pipe(Effect.as({ slept: ms }))),
}) {}

export class Flaky extends Agent.make<Flaky>()("Flaky", {
  retry: { maxAttempts: 3, delay: "200 millis" },
  tasks: {
    // Fails with a retryable tagged error while attempt <= failTimes.
    flaky: Agent.task({ failTimes: Schema.Number }, ({ failTimes }) =>
      Effect.gen(function* () {
        const { attempt } = yield* Agent.AgentContext;
        if (attempt <= failTimes) return yield* new Flake({ message: `flake #${attempt}` });
        return { attempt };
      }),
    ),
    permanent: Agent.task(() => Effect.fail(new Agent.PermanentError({ message: "nope" }))),
    die: Agent.task(() => Effect.die(new Error("boom"))),
    // Ends the execution abruptly while attempt <= times (like an eviction mid-task).
    crash: Agent.task({ times: Schema.Number }, ({ times }) =>
      Effect.gen(function* () {
        const { attempt } = yield* Agent.AgentContext;
        if (attempt <= times) return yield* Testing.crashExecution;
        return { attempt };
      }),
    ),
  },
}) {}

// Agent B sends to agent A, which it declares in `uses`.
export class Relay extends Agent.make<Relay>()("Relay", {
  uses: [Echo],
  tasks: {
    forward: Agent.task({ text: Schema.String }, ({ text }) =>
      Effect.gen(function* () {
        const ctx = yield* Agent.AgentContext;
        const handle = yield* Echo.named(`from-${ctx.agent.name}`).send.echo({ text }, { taskId: `${ctx.taskId}:echo` });
        return { taskId: handle.taskId };
      }),
    ),
  },
}) {}
