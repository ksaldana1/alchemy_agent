// Retry policies as schedules, stepped durably (internal/retry.ts). Pure: no workerd.
import { describe, expect, test } from "bun:test";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Retry from "../src/internal/retry.ts";

const run = <A>(effect: Effect.Effect<A>) => Effect.runSync(effect);
const delays = (schedule: Schedule.Schedule<any, unknown>, failures: number) =>
  Array.from({ length: failures }, (_, i) => run(Retry.delayAfter(schedule, i + 1, undefined)));

describe("retry policies", () => {
  test("the default: 3 attempts, 1 s doubling", () => {
    const schedule = Retry.resolve([]);
    expect(run(Retry.maxAttempts(schedule))).toBe(3);
    expect(delays(schedule, 3)).toEqual([1000, 2000, undefined]);
  });

  test("shorthand options merge over the agent's and the defaults; maxDelay caps the backoff", () => {
    const schedule = Retry.resolve([{ delay: "1 second", maxDelay: "3 seconds" }, { maxAttempts: 5 }]);
    expect(run(Retry.maxAttempts(schedule))).toBe(5);
    expect(delays(schedule, 5)).toEqual([1000, 2000, 3000, 3000, undefined]);
  });

  test("maxAttempts: 1 never retries", () => {
    const schedule = Retry.resolve([{ maxAttempts: 1 }]);
    expect(run(Retry.maxAttempts(schedule))).toBe(1);
    expect(run(Retry.delayAfter(schedule, 1, undefined))).toBeUndefined();
  });

  test("the most specific schedule wins, and decides when retries end", () => {
    const schedule = Retry.resolve([{ maxAttempts: 10 }, Schedule.spaced("30 seconds").pipe(Schedule.upTo({ times: 2 }))]);
    expect(run(Retry.maxAttempts(schedule))).toBe(3);
    expect(delays(schedule, 3)).toEqual([30_000, 30_000, undefined]);
  });

  test("a schedule sees the failure as its input", () => {
    const schedule = Schedule.spaced("1 second").pipe(
      Schedule.upTo({ times: 5 }),
      Schedule.modifyDelay(({ input, duration }) => Effect.succeed(input === "slow" ? "1 minute" : duration)),
    );
    expect(run(Retry.delayAfter(schedule, 1, "slow"))).toBe(60_000);
    expect(run(Retry.delayAfter(schedule, 1, "fast"))).toBe(1000);
  });

  test("a schedule that never ends stops at the attempt cap", () => {
    const schedule = Schedule.spaced("1 second");
    expect(run(Retry.maxAttempts(schedule))).toBe(Retry.ATTEMPT_CAP);
    expect(run(Retry.delayAfter(schedule, Retry.ATTEMPT_CAP, undefined))).toBeUndefined();
  });
});
