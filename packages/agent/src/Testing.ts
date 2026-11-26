// Test-only hooks: `import * as Testing from "alchemy-agent/testing"`.
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChaosPlan, CurrentExecution, type CrashRule } from "./internal/core.ts";

export type { CrashRule };

/**
 * Plans a crash: the first delivery of `task` to an agent of this type (any name, or `name`)
 * dies as its handler starts, the way an eviction would. The task is delivered again.
 */
export const crashOnce = (agent: { readonly type: string }, task: string, options?: { readonly name?: string }): CrashRule => ({
  type: agent.type,
  task,
  name: options?.name,
});

/** The planned crashes, for a test platform: `Platform.make({ ..., provide: Testing.chaos([...]) })`. */
export const chaos = (rules: ReadonlyArray<CrashRule>): Layer.Layer<any> => Layer.succeed(ChaosPlan, rules) as unknown as Layer.Layer<any>;

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
