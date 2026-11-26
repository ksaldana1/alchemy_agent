// Test agents. Each task exercises one inbox behavior. Contracts first, then implementations.
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";
import * as Agent from "../../src/Agent.ts";
import * as Testing from "../../src/Testing.ts";

// ---------------------------------------------------------------------------
// Contracts
// ---------------------------------------------------------------------------

export const EchoText = Rpc.make("EchoText", {
  payload: { text: Schema.String },
  success: Schema.Struct({ echoed: Schema.String }),
});
/** Keyed by `id`: sending the same id again is the same task. */
export const EchoKeyed = Rpc.make("EchoKeyed", {
  payload: { id: Schema.String, text: Schema.String },
  success: Schema.Struct({ echoed: Schema.String }),
  primaryKey: ({ id }) => id,
});
export const Whoami = Rpc.make("Whoami", {
  success: Schema.Struct({ type: Schema.String, name: Schema.String, taskId: Schema.String, attempt: Schema.Number }),
});
/** Execution order within one isolate: proves FIFO. */
export const Order = Rpc.make("Order", {
  payload: { label: Schema.String },
  success: Schema.Struct({ label: Schema.String, n: Schema.Number }),
  primaryKey: ({ label }) => label,
});
/** A success schema with a transformation: a Date in the handler, an ISO string on the wire. */
export const Stamp = Rpc.make("Stamp", {
  payload: { epochMs: Schema.Number },
  success: Schema.Struct({ at: Schema.DateFromString }),
});
export const Slow = Rpc.make("Slow", { payload: { ms: Schema.Number }, success: Schema.Struct({ slept: Schema.Number }) });
export const Echo = Agent.make("Echo", [EchoText, EchoKeyed, Whoami, Order, Stamp, Slow]);

export class Nope extends Schema.TaggedError<Nope>()("Nope", { reason: Schema.String }) {}
export const FlakyTask = Rpc.make("FlakyTask", { payload: { failTimes: Schema.Number }, success: Schema.Struct({ attempt: Schema.Number }) });
/** Fails with a declared error: settles at once, and the caller gets `Nope`. */
export const Refuse = Rpc.make("Refuse", { error: Nope });
export const Die = Rpc.make("Die");
export const Crash = Rpc.make("Crash", { payload: { times: Schema.Number }, success: Schema.Struct({ attempt: Schema.Number }) });
export const Flaky = Agent.make("Flaky", [FlakyTask, Refuse, Die, Crash]);

export const Forward = Rpc.make("Forward", { payload: { text: Schema.String }, success: Schema.Struct({ taskId: Schema.String }) });
/** Waits for another agent from inside a handler: not possible until M3. */
export const AskEcho = Rpc.make("AskEcho", { payload: { text: Schema.String }, success: Schema.String });
export const Relay = Agent.make("Relay", [Forward, AskEcho]);

// ---------------------------------------------------------------------------
// Implementations
// ---------------------------------------------------------------------------

class Flake extends Data.TaggedError("Flake")<{ readonly message: string }> {}

let order = 0;

export const EchoLive = Echo.toLayer(
  Echo.of({
    EchoText: ({ payload }) => Effect.succeed({ echoed: payload.text.toUpperCase() }),
    EchoKeyed: ({ payload }) => Effect.succeed({ echoed: payload.text.toUpperCase() }),
    Whoami: ({ agent, taskId, attempt }) => Effect.succeed({ type: agent.type, name: agent.name, taskId, attempt }),
    Order: ({ payload }) => Effect.sync(() => ({ label: payload.label, n: ++order })),
    Stamp: ({ payload }) => Effect.succeed({ at: new Date(payload.epochMs) }),
    Slow: ({ payload }) => Effect.sleep(`${payload.ms} millis`).pipe(Effect.as({ slept: payload.ms })),
  }),
);

export const FlakyLive = Flaky.toLayer(
  Flaky.of({
    // Fails with an undeclared error while attempt <= failTimes: retried.
    FlakyTask: ({ payload, attempt }) =>
      attempt <= payload.failTimes ? Effect.fail(new Flake({ message: `flake #${attempt}` })) : Effect.succeed({ attempt }),
    Refuse: () => Effect.fail(new Nope({ reason: "never" })),
    Die: () => Effect.die(new Error("boom")),
    // Ends the execution abruptly while attempt <= times (like an eviction mid-task).
    Crash: ({ payload, attempt }) => (attempt <= payload.times ? Testing.crashExecution : Effect.succeed({ attempt })),
  }),
  { retry: { maxAttempts: 3, delay: "200 millis" } },
);

// Another agent's client is just `yield* Echo.client` while building: no declaration needed.
export const RelayLive = Relay.toLayer(
  Effect.gen(function* () {
    const echoes = yield* Echo.client;
    return Relay.of({
      Forward: Effect.fn("Relay.Forward")(function* ({ payload, agent, taskId }) {
        const id = `${taskId}:echo`;
        yield* echoes(`from-${agent.name}`).EchoKeyed({ id, text: payload.text }, { discard: true });
        return { taskId: `EchoKeyed:${id}` };
      }),
      AskEcho: ({ payload }) => Effect.map(echoes("asked").EchoText({ text: payload.text }), (r) => r.echoed),
    });
  }),
);
