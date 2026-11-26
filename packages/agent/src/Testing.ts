// Test-only hooks: `import * as Testing from "alchemy-agent/testing"`.
import * as Effect from "effect/Effect";
import { CurrentExecution } from "./internal/core.ts";

/**
 * Ends the current execution abruptly, as an eviction or a CPU-limit reset would: the Durable
 * Object is reset and the task is left `running`, so the next drain counts a lost attempt.
 */
export const crashExecution: Effect.Effect<never> = Effect.gen(function* () {
  const execution = yield* CurrentExecution;
  if (execution === undefined) return yield* Effect.die(new Error("not inside a task"));
  execution.abort("Testing.crashExecution");
  return yield* Effect.never;
});

/**
 * Increments a durable counter in the agent's Durable Object and returns the new value. It
 * survives crashes and resets, so tests can count how often a handler or tool really ran.
 */
export const count: (name: string) => Effect.Effect<number> = Effect.fnUntraced(function* (name: string) {
  const execution = yield* CurrentExecution;
  if (execution === undefined) return yield* Effect.die(new Error("not inside a task or tool"));
  return yield* execution.count(name);
});
