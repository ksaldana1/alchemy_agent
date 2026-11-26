// Retry policies as Effect `Schedule`s, stepped durably. A task's retries outlive any execution,
// so no schedule state is kept between failures: each failure replays the schedule from its start
// on a clock that advances by the schedule's own delays, and takes the delay of the n-th step.
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Pull from "effect/Pull";
import * as Schedule from "effect/Schedule";
import type { RetryOptions, RetryPolicy } from "./core.ts";

/** Attempts a schedule that never ends is allowed (every task settles eventually). */
export const ATTEMPT_CAP = 100;

const DEFAULTS = { maxAttempts: 3, delay: "1 second", maxDelay: "5 minutes" } as const satisfies RetryOptions;

/** The shorthand as a schedule: exponential from `delay`, capped at `maxDelay`, `maxAttempts - 1` retries. */
const fromOptions = (options: RetryOptions): Schedule.Schedule<any, unknown> => {
  const o = { ...DEFAULTS, ...options };
  const cap = Duration.fromInputUnsafe(o.maxDelay);
  return Schedule.exponential(o.delay).pipe(
    Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.min(duration, cap))),
    Schedule.upTo({ times: Math.max(0, o.maxAttempts - 1) }),
  );
};

/**
 * The policy for a task: the most specific schedule wins (the task's `Agent.Retry`, then the
 * agent's `retry`), and shorthand options merge over the less specific shorthand and the defaults.
 */
export const resolve = (policies: ReadonlyArray<RetryPolicy | undefined>): Schedule.Schedule<any, unknown> => {
  let resolved: RetryPolicy = DEFAULTS;
  for (const p of policies) {
    if (p === undefined) continue;
    resolved = Schedule.isSchedule(p) || Schedule.isSchedule(resolved) ? p : { ...resolved, ...p };
  }
  return Schedule.isSchedule(resolved) ? resolved : fromOptions(resolved);
};

/**
 * The delays of the schedule's first `steps` steps, each with `input`; shorter if it ends first.
 * The clock starts at 0 and advances by each delay.
 */
const delays = (schedule: Schedule.Schedule<any, unknown>, steps: number, input: unknown) =>
  Effect.gen(function* () {
    const step = yield* Schedule.toStep(schedule);
    const out: Array<number> = [];
    let now = 0;
    for (let i = 0; i < steps; i++) {
      const next = yield* step(now, input).pipe(
        Effect.map(([, delay]) => Duration.toMillis(delay)),
        Pull.catchDone(() => Effect.succeed(undefined)),
      );
      if (next === undefined) break;
      out.push(next);
      now += next;
    }
    return out;
  }) as Effect.Effect<Array<number>>;

/** Deliveries the schedule allows: the first, plus one per step before it ends (capped). */
export const maxAttempts = (schedule: Schedule.Schedule<any, unknown>): Effect.Effect<number> =>
  Effect.map(delays(schedule, ATTEMPT_CAP - 1, undefined), (d) => 1 + d.length);

/**
 * After failed attempt number `attempts` (1-based), how long to wait before the next delivery, or
 * undefined when the schedule has ended. `failure` is the schedule's input for every step.
 */
export const delayAfter = (schedule: Schedule.Schedule<any, unknown>, attempts: number, failure: unknown): Effect.Effect<number | undefined> =>
  attempts >= ATTEMPT_CAP
    ? Effect.succeed(undefined)
    : Effect.map(delays(schedule, attempts, failure), (d) => (d.length < attempts ? undefined : d[attempts - 1]));
