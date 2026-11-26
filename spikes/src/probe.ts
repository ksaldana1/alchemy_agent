// Spike 0.3: a Durable Object that records what Alchemy's durable callbacks actually do.
// Every experiment reads the append-only `log` table back through `log()`.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";

// Bumped between deploys for E5 (does a pending callback run the new code?).
const VERSION = "v2";
// Set to false and redeploy for E6 (what happens to a job whose callback name is gone?).
const REGISTER_ORPHAN = true;

export interface WorkPayload {
  readonly id: string;
  /** Fail while the stored attempt count is at or below this. */
  readonly failTimes?: number;
  /** "tagged" fails with a tagged error; "die" fails with a defect. */
  readonly failMode?: "tagged" | "die";
  /** Wait this long (a timer, so other events may run) before finishing. */
  readonly sleepMs?: number;
  /** Busy-loop for this many iterations (CPU time; calibrated in spikes/scripts/probe.ts). */
  readonly cpuIters?: number;
  /** Reschedule itself this many more times (E9). */
  readonly chain?: number;
  /** Report a strictly increasing `progress` when rescheduling (E9b). */
  readonly withProgress?: boolean;
  readonly hop?: number;
  readonly note?: string;
}

export interface ScheduleOptions {
  readonly afterMs?: number;
  readonly payload: WorkPayload;
  readonly callback?: "work" | "flaky" | "orphan";
}

class WorkFailed extends Data.TaggedError("WorkFailed")<{ readonly attempt: number }> {}
class TxAborted extends Data.TaggedError("TxAborted")<{}> {}

type LogRow = {
  seq: number;
  at: number;
  event: string;
  id: string;
  attempt: number;
  detail: string;
};

export default class Probe extends Cloudflare.DurableObject<Probe>()(
  "Probe",
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    return Effect.gen(function* () {
      const sql = state.storage.sql;
      yield* sql.exec(`CREATE TABLE IF NOT EXISTS log (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER, event TEXT, id TEXT,
        attempt INTEGER, detail TEXT)`);
      yield* sql.exec(`CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY, n INTEGER)`);
      // Drain-loop proof: a tiny inbox owned by this DO.
      yield* sql.exec(`CREATE TABLE IF NOT EXISTS inbox (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT UNIQUE, status TEXT, runs INTEGER)`);

      const log = (event: string, id: string, attempt = 0, detail: unknown = "") =>
        sql
          .exec(
            "INSERT INTO log (at, event, id, attempt, detail) VALUES (?, ?, ?, ?, ?)",
            Date.now(),
            event,
            id,
            attempt,
            typeof detail === "string" ? detail : JSON.stringify(detail),
          )
          .pipe(Effect.asVoid);

      const bumpAttempt = (id: string) =>
        sql
          .exec<{ n: number }>(
            "INSERT INTO attempts (id, n) VALUES (?, 1) ON CONFLICT (id) DO UPDATE SET n = n + 1 RETURNING n",
            id,
          )
          .pipe(Effect.flatMap((c) => c.one()), Effect.map((r) => r.n));

      // `work` is referenced inside its own handler (self-rescheduling, getStatus).
      let work: Alchemy.Callback<WorkPayload>;

      const handler = (callbackName: string) => (p: WorkPayload) =>
        Effect.gen(function* () {
          const attempt = yield* bumpAttempt(`${callbackName}:${p.id}`);
          const status = yield* work.getStatus(p.id).pipe(Effect.orElseSucceed(() => undefined));
          yield* log("start", p.id, attempt, {
            v: VERSION,
            cb: callbackName,
            hop: p.hop,
            note: p.note,
            alchemyAttempts: status?.attempts,
          });
          if (p.cpuIters) {
            // Count iterations, not time: deployed Workers freeze Date.now() during synchronous
            // code, so a clock-based busy loop never ends.
            const t0 = performance.now();
            let x = 0;
            for (let i = 0; i < p.cpuIters; i++) x += Math.sqrt(i + x);
            yield* log("cpu-done", p.id, attempt, {
              cpuIters: p.cpuIters,
              localMs: Math.round(performance.now() - t0),
              x: x > 0,
            });
          }
          if (p.sleepMs) yield* Effect.sleep(`${p.sleepMs} millis`);
          if (attempt <= (p.failTimes ?? 0)) {
            yield* log("fail", p.id, attempt, p.failMode ?? "tagged");
            if (p.failMode === "die") return yield* Effect.die(new Error(`die #${attempt}`));
            return yield* new WorkFailed({ attempt });
          }
          if (p.chain && p.chain > 0 && callbackName === "work") {
            const hop = (p.hop ?? 0) + 1;
            yield* work.schedule(p.id, {
              after: 0,
              payload: { ...p, chain: p.chain - 1, hop },
              ...(p.withProgress ? { progress: hop } : {}),
            });
          }
          yield* log("end", p.id, attempt, { v: VERSION });
        });

      work = yield* Alchemy.makeCallback("work", handler("work"), {
        retry: { delay: "2 seconds" },
      });
      const flaky = yield* Alchemy.makeCallback("flaky", handler("flaky"), {
        retry: { delay: "1 second", maxAttempts: 3 },
      });
      const orphan = REGISTER_ORPHAN
        ? yield* Alchemy.makeCallback("orphan", handler("orphan"), {
            retry: { delay: "2 seconds" },
          })
        : undefined;

      // Drain loop: enqueue writes a row and schedules "drain" in the same transaction.
      // The drain processes a batch, then reschedules itself if rows are left, reporting
      // the highest processed seq as progress so the retry budget never runs out.
      const BATCH = 3;
      let drain: Alchemy.Callback<{ readonly v: 1 }>;
      drain = yield* Alchemy.makeCallback(
        "drain",
        (_: { readonly v: 1 }) =>
          Effect.gen(function* () {
            const rows = yield* sql
              .exec<{ seq: number; task_id: string }>(
                "SELECT seq, task_id FROM inbox WHERE status = 'pending' ORDER BY seq LIMIT ?",
                BATCH,
              )
              .pipe(Effect.flatMap((c) => c.toArray()));
            yield* log("drain", "drain", 0, { batch: rows.map((r) => r.task_id) });
            for (const row of rows) {
              yield* state.storage.transaction(
                sql.exec(
                  "UPDATE inbox SET status = 'done', runs = runs + 1 WHERE seq = ?",
                  row.seq,
                ),
              );
            }
            const left = yield* sql
              .exec<{ n: number }>("SELECT count(*) AS n FROM inbox WHERE status = 'pending'")
              .pipe(Effect.flatMap((c) => c.one()));
            const last = rows.at(-1);
            if (left.n > 0 && last) {
              yield* drain.schedule("drain", { after: 0, payload: { v: 1 }, progress: last.seq });
            }
          }),
        { retry: { delay: "2 seconds" } },
      );

      const pick = (name: ScheduleOptions["callback"]) =>
        name === "flaky" ? flaky : name === "orphan" ? (orphan ?? work) : work;

      const readLog = () =>
        sql
          .exec<LogRow>("SELECT * FROM log ORDER BY seq")
          .pipe(Effect.flatMap((c) => c.toArray()));

      return {
        schedule: (opts: ScheduleOptions) =>
          Effect.gen(function* () {
            yield* log("scheduled", opts.payload.id, 0, {
              cb: opts.callback ?? "work",
              afterMs: opts.afterMs ?? 0,
            });
            yield* pick(opts.callback).schedule(opts.payload.id, {
              after: `${opts.afterMs ?? 0} millis`,
              payload: opts.payload,
            });
            return { ok: true as const };
          }),

        // E2: write + schedule in one transaction, then fail it.
        scheduleInTxThenFail: (id: string) =>
          state.storage
            .transaction(
              Effect.gen(function* () {
                yield* log("tx-write", id);
                yield* work.schedule(id, { after: "1 second", payload: { id, note: "tx" } });
                return yield* new TxAborted();
              }),
            )
            .pipe(
              Effect.catchTag("TxAborted", () => Effect.succeed({ aborted: true as const })),
              Effect.catchTag("CallbackError", (e) =>
                Effect.succeed({ aborted: false as const, error: e.message }),
              ),
            ),

        // E7: n callbacks with after 0 in one transaction.
        scheduleMany: (n: number) =>
          state.storage.transaction(
            Effect.gen(function* () {
              for (let i = 0; i < n; i++) {
                const id = `m${String(i).padStart(3, "0")}`;
                yield* work.schedule(id, { after: 0, payload: { id } });
              }
              yield* log("scheduled-many", "-", 0, { n });
              return { ok: true as const };
            }),
          ),

        // E12: payloads that aren't JSON.
        scheduleBad: (kind: "date" | "undefined") =>
          work
            .schedule(`bad-${kind}`, {
              after: "1 second",
              payload: (kind === "date"
                ? { id: `bad-${kind}`, note: new Date() }
                : { id: `bad-${kind}`, note: undefined }) as unknown as WorkPayload,
            })
            .pipe(
              Effect.as({ rejected: false as const, error: "" }),
              Effect.catchTag("CallbackError", (e) =>
                Effect.succeed({ rejected: true as const, error: e.message }),
              ),
              Effect.catchDefect((d) =>
                Effect.succeed({ rejected: true as const, error: `defect: ${String(d)}` }),
              ),
            ),

        cancel: (id: string, callback?: ScheduleOptions["callback"]) =>
          pick(callback).cancel(id).pipe(Effect.as({ ok: true as const })),
        status: (id: string, callback?: ScheduleOptions["callback"]) =>
          pick(callback).getStatus(id).pipe(Effect.map((s) => s ?? null)),

        // E8: an RPC call that only logs, to see if it interleaves with a running callback.
        ping: (id: string) => log("rpc", id).pipe(Effect.as({ ok: true as const })),

        // Drain loop.
        enqueue: (taskIds: ReadonlyArray<string>) =>
          state.storage.transaction(
            Effect.gen(function* () {
              for (const taskId of taskIds) {
                yield* sql.exec(
                  "INSERT INTO inbox (task_id, status, runs) VALUES (?, 'pending', 0) ON CONFLICT (task_id) DO NOTHING",
                  taskId,
                );
              }
              yield* drain.schedule("drain", { after: 0, payload: { v: 1 } });
              return { ok: true as const };
            }),
          ),
        inbox: () =>
          sql
            .exec<{ seq: number; task_id: string; status: string; runs: number }>(
              "SELECT * FROM inbox ORDER BY seq",
            )
            .pipe(Effect.flatMap((c) => c.toArray())),

        // Alchemy's own job table (internal; read-only here). Shows jobs no handle can see,
        // such as one whose callback name is no longer registered (E6).
        jobs: () =>
          sql
            .exec<Record<string, string | number | null>>(
              "SELECT callback, id, run_at, scheduled_at, retry_at, attempts, parked, progress FROM alchemy_alarm_callbacks ORDER BY run_at",
            )
            .pipe(Effect.flatMap((c) => c.toArray())),

        log: readLog,
        reset: () =>
          Effect.gen(function* () {
            yield* sql.exec("DELETE FROM log");
            yield* sql.exec("DELETE FROM attempts");
            yield* sql.exec("DELETE FROM inbox");
            return { ok: true as const };
          }),
      };
    });
  }),
) {}
