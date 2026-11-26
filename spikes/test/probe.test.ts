import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import Stack from "../alchemy.run.ts";

// Spike 0.3 regression tests: the durable-callback behavior that D4 relies on.
// The full experiment set (E1–E12) lives in spikes/scripts/probe.ts.
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

const run = crypto.randomUUID().slice(0, 8);

type Row = { event: string; id: string; attempt: number; detail: string };

const call = (url: string | undefined, probe: string, method: string, ...args: unknown[]) =>
  Test.executeWhenReady(
    args.length
      ? HttpClientRequest.post(`${url}/probe/${probe}/${method}`).pipe(
          HttpClientRequest.setBody(HttpBody.jsonUnsafe(args)),
        )
      : HttpClientRequest.get(`${url}/probe/${probe}/${method}`),
  ).pipe(Effect.flatMap((res) => res.json));

/** Poll `method` until `done` holds; fail after ~20 s. */
const until = <A>(effect: Effect.Effect<unknown, unknown, any>, done: (a: A) => boolean) =>
  effect.pipe(
    Effect.map((a) => a as A),
    Effect.repeat({ schedule: Schedule.spaced("300 millis"), until: done, times: 70 }),
  );

test(
  "E2: a callback scheduled in a failed transaction never fires",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const p = `t-e2-${run}`;
    expect(yield* call(url, p, "scheduleInTxThenFail", "tx")).toEqual({ aborted: true });
    expect(yield* call(url, p, "status", "tx")).toBeNull();
    yield* Effect.sleep("3 seconds");
    expect(yield* call(url, p, "log")).toEqual([]);
  }),
  { timeout: 60_000 },
);

test(
  "E4: scheduling the same id again replaces the pending job",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const p = `t-e4-${run}`;
    yield* call(url, p, "schedule", { afterMs: 1000, payload: { id: "same", note: "first" } });
    yield* call(url, p, "schedule", { afterMs: 1000, payload: { id: "same", note: "second" } });
    const rows = yield* until<Row[]>(call(url, p, "log"), (r) =>
      r.some((x) => x.event === "end"),
    );
    const starts = rows.filter((r) => r.event === "start");
    expect(starts).toHaveLength(1);
    expect(JSON.parse(starts[0]!.detail).note).toBe("second");
  }),
  { timeout: 60_000 },
);

test(
  "E9: self-rescheduling needs increasing progress, or it backs off",
  Effect.gen(function* () {
    const { url } = yield* stack;
    // With progress: 5 hops run back to back.
    const fast = `t-e9p-${run}`;
    yield* call(url, fast, "schedule", { payload: { id: "c", chain: 5, withProgress: true } });
    const rows = yield* until<Row[]>(
      call(url, fast, "log"),
      (r) => r.filter((x) => x.event === "end").length >= 6,
    );
    expect(rows.filter((r) => r.event === "end")).toHaveLength(6);

    // Without progress: the second hop waits for the 2 s retry delay, the third for 4 s.
    const slow = `t-e9-${run}`;
    yield* call(url, slow, "schedule", { payload: { id: "c", chain: 5 } });
    yield* Effect.sleep("3 seconds");
    const status = (yield* call(url, slow, "status", "c")) as { attempts: number } | null;
    expect(status?.attempts).toBeGreaterThanOrEqual(2);
    const ends = ((yield* call(url, slow, "log")) as Row[]).filter((r) => r.event === "end");
    expect(ends.length).toBeLessThan(6);
  }),
  { timeout: 60_000 },
);

test(
  "drain loop: concurrent enqueues collapse into one drain; every row runs once",
  Effect.gen(function* () {
    const { url } = yield* stack;
    const p = `t-drain-${run}`;
    yield* Effect.all(
      [
        call(url, p, "enqueue", ["t01", "t02", "t03", "t04"]),
        call(url, p, "enqueue", ["t05", "t06", "t07", "t08"]),
        call(url, p, "enqueue", ["t09", "t10", "t11", "t04"]),
      ],
      { concurrency: "unbounded" },
    );
    const inbox = yield* until<Array<{ status: string; runs: number }>>(
      call(url, p, "inbox"),
      (rows) => rows.length === 11 && rows.every((r) => r.status === "done"),
    );
    expect(inbox).toHaveLength(11);
    expect(inbox.every((r) => r.runs === 1)).toBe(true);
  }),
  { timeout: 60_000 },
);
