// The inbox: one SQLite table inside each agent's Durable Object, owned by framework code.
// Every function here is a storage operation; the drain loop in `Agent.ts` sequences them.
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/** The `SqlStorage` wrapper from `Cloudflare.DurableObjectState`'s storage. */
export interface Sql {
  exec<T extends Record<string, string | number | null | ArrayBuffer>>(
    query: string,
    ...bindings: Array<unknown>
  ): Effect.Effect<
    {
      toArray(): Effect.Effect<Array<T>, never, RuntimeContext>;
      one(): Effect.Effect<T, never, RuntimeContext>;
    },
    never,
    RuntimeContext
  >;
}

/**
 * `waiting`: suspended on an LLM turn or on another agent's answer; it resumes as `pending` when
 * that settles.
 */
export type TaskStatus = TaskRecord["status"];

/**
 * A failure as stored, and as it crosses the wire:
 * - `declared`: one of the task's declared errors, encoded with its Rpc error schema; the caller
 *   decodes it back into the real error
 * - `agent`: an `AgentError` reason (encoded), e.g. retries exhausted or execution lost
 * - `attempt`: the last retryable failure of a task that will be delivered again
 */
export const StoredFailure = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("declared"), value: Schema.Unknown }),
  Schema.Struct({ kind: Schema.Literal("agent"), reason: Schema.Unknown }),
  Schema.Struct({ kind: Schema.Literal("attempt"), message: Schema.String }),
]);
export type StoredFailure = typeof StoredFailure.Type;

/** A task as stored in the inbox and returned over RPC and HTTP. */
export const TaskRecord = Schema.Struct({
  taskId: Schema.String,
  task: Schema.String,
  status: Schema.Literals(["pending", "running", "waiting", "completed", "failed"]),
  attempts: Schema.Number,
  maxAttempts: Schema.Number,
  /** The handler's output, encoded with the task's success schema. Present when `completed`. */
  result: Schema.optionalKey(Schema.Unknown),
  /** The failure. Present when `failed`, and on a `pending` task that will retry. */
  error: Schema.optionalKey(StoredFailure),
  causation: Schema.NullOr(Schema.String),
  createdAt: Schema.Number,
  settledAt: Schema.NullOr(Schema.Number),
});

export type TaskRecord = typeof TaskRecord.Type;

type Row = {
  seq: number;
  task_id: string;
  task: string;
  input: string;
  status: string;
  attempts: number;
  max_attempts: number;
  run_at: number;
  result: string | null;
  error: string | null;
  causation: string | null;
  created_at: number;
  started_at: number | null;
  settled_at: number | null;
};

export type InboxRow = Row;

// Versioned schema, applied on activation. Each entry runs once, in order, inside a
// transaction with its version bump. Never edit an applied entry; append a new one.
const MIGRATIONS: ReadonlyArray<string> = [
  `CREATE TABLE alchemy_agent_inbox (
     seq          INTEGER PRIMARY KEY AUTOINCREMENT,
     task_id      TEXT    NOT NULL UNIQUE,
     task         TEXT    NOT NULL,
     input        TEXT    NOT NULL,
     status       TEXT    NOT NULL,
     attempts     INTEGER NOT NULL DEFAULT 0,
     max_attempts INTEGER NOT NULL,
     run_at       INTEGER NOT NULL,
     result       TEXT,
     error        TEXT,
     causation    TEXT,
     created_at   INTEGER NOT NULL,
     started_at   INTEGER,
     settled_at   INTEGER
   );
   CREATE INDEX alchemy_agent_inbox_due ON alchemy_agent_inbox (status, run_at, seq);
   CREATE TABLE alchemy_agent_meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
   INSERT INTO alchemy_agent_meta (key, value) VALUES ('drain_cursor', 0);`,
  // M2: LLM sessions. A turn is one prompt; it runs as a sequence of persisted steps.
  `CREATE TABLE alchemy_agent_turns (
     id            TEXT    PRIMARY KEY,
     session_id    TEXT    NOT NULL,
     task_id       TEXT    NOT NULL,
     status        TEXT    NOT NULL,
     message       TEXT    NOT NULL,
     output_schema TEXT,
     steps         INTEGER NOT NULL DEFAULT 0,
     errors        INTEGER NOT NULL DEFAULT 0,
     ticks         INTEGER NOT NULL DEFAULT 0,
     tokens_in     INTEGER NOT NULL DEFAULT 0,
     tokens_out    INTEGER NOT NULL DEFAULT 0,
     output        TEXT,
     error         TEXT,
     created_at    INTEGER NOT NULL,
     started_at    INTEGER,
     settled_at    INTEGER
   );
   CREATE INDEX alchemy_agent_turns_session ON alchemy_agent_turns (session_id, status, created_at);
   CREATE TABLE alchemy_agent_transcript (
     session_id TEXT    NOT NULL,
     seq        INTEGER NOT NULL,
     turn_id    TEXT    NOT NULL,
     kind       TEXT    NOT NULL,
     message    TEXT    NOT NULL,
     created_at INTEGER NOT NULL,
     PRIMARY KEY (session_id, seq)
   );
   CREATE TABLE alchemy_agent_counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);`,
  // API reset: a turn's system prompt (per `Agent.generate` call).
  `ALTER TABLE alchemy_agent_turns ADD COLUMN system TEXT;`,
  // M3: messages between agents. `outbox` holds calls this agent makes and replies it owes;
  // `subscribers` holds who waits for which of this agent's tasks.
  `CREATE TABLE alchemy_agent_outbox (
     id          TEXT    PRIMARY KEY,
     kind        TEXT    NOT NULL,
     owner_kind  TEXT,
     owner_id    TEXT,
     target_type TEXT    NOT NULL,
     target_name TEXT    NOT NULL,
     task_id     TEXT    NOT NULL,
     tag         TEXT,
     payload     TEXT,
     reply_id    TEXT,
     wait        INTEGER NOT NULL DEFAULT 0,
     causation   TEXT,
     status      TEXT    NOT NULL,
     record      TEXT,
     attempts    INTEGER NOT NULL DEFAULT 0,
     run_at      INTEGER NOT NULL,
     created_at  INTEGER NOT NULL
   );
   CREATE INDEX alchemy_agent_outbox_due ON alchemy_agent_outbox (status, run_at);
   CREATE TABLE alchemy_agent_subscribers (
     task_id    TEXT NOT NULL,
     reply_type TEXT NOT NULL,
     reply_name TEXT NOT NULL,
     reply_id   TEXT NOT NULL,
     PRIMARY KEY (task_id, reply_type, reply_name, reply_id)
   );`,
  // Retention: settled tasks are pruned oldest first.
  `CREATE INDEX alchemy_agent_inbox_settled ON alchemy_agent_inbox (settled_at) WHERE settled_at IS NOT NULL;`,
];

export const migrate = Effect.fnUntraced(function* (
  sql: Sql,
  transaction: <A>(effect: Effect.Effect<A, never, RuntimeContext>) => Effect.Effect<A, never, RuntimeContext>,
) {
    yield* sql.exec(
      "CREATE TABLE IF NOT EXISTS alchemy_agent_schema (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)",
    );
    const rows = yield* (yield* sql.exec<{ version: number }>(
      "SELECT version FROM alchemy_agent_schema WHERE id = 1",
    )).toArray();
    const current = rows[0]?.version ?? 0;
    if (current > MIGRATIONS.length) {
      return yield* Effect.die(
        new Error(
          `Inbox schema version ${current} is newer than this code supports (${MIGRATIONS.length})`,
        ),
      );
    }
    for (let v = current; v < MIGRATIONS.length; v++) {
      yield* transaction(
        Effect.gen(function* () {
          yield* sql.exec(MIGRATIONS[v]!);
          yield* sql.exec(
            "INSERT INTO alchemy_agent_schema (id, version) VALUES (1, ?) ON CONFLICT (id) DO UPDATE SET version = excluded.version",
            v + 1,
          );
        }),
      );
    }
  });

const parse = (text: string | null): unknown => (text === null ? undefined : JSON.parse(text));

export const toRecord = (row: Row): TaskRecord => {
  const result = parse(row.result);
  const error = parse(row.error) as StoredFailure | undefined;
  return {
    taskId: row.task_id,
    task: row.task,
    status: row.status as TaskStatus,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    ...(row.status === "completed" ? { result: result ?? null } : {}),
    ...(error === undefined ? {} : { error }),
    causation: row.causation,
    createdAt: row.created_at,
    settledAt: row.settled_at,
  };
};

const one = (sql: Sql, query: string, ...bindings: Array<unknown>) =>
  Effect.flatMap(sql.exec<Row>(query, ...bindings), (c) => c.toArray()).pipe(
    Effect.map((rows) => rows[0]),
  );

export const get = (sql: Sql, taskId: string) =>
  one(sql, "SELECT * FROM alchemy_agent_inbox WHERE task_id = ?", taskId);

export interface NewTask {
  readonly taskId: string;
  readonly task: string;
  readonly input: unknown;
  readonly maxAttempts: number;
  readonly causation: string | null;
  readonly now: number;
  /** Set when the task is settled on arrival (unknown task, invalid input). */
  readonly failure?: StoredFailure;
}

/** JSON with object keys sorted, so two payloads compare equal whatever their key order. */
const canonical = (value: unknown): string =>
  JSON.stringify(value ?? null, (_, v: unknown) =>
    v !== null && typeof v === "object" && !Array.isArray(v)
      ? Object.fromEntries(Object.entries(v as Record<string, unknown>).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : v,
  );

/**
 * Why an existing task can't be the one `t` asks for, or undefined if it is the same task. The
 * task id is an idempotency key: sending it again with another task or payload is a mistake, not
 * a duplicate.
 */
export const conflictWith = (row: Row, t: { readonly task: string; readonly input: unknown }): string | undefined =>
  row.task !== t.task
    ? `task id ${row.task_id} is already recorded for ${row.task}, not ${t.task}`
    : canonical(JSON.parse(row.input)) !== canonical(t.input)
      ? `task id ${row.task_id} is already recorded with a different payload`
      : undefined;

/**
 * Records a task. A task id that already exists is left untouched (the task id is the
 * idempotency key). Returns the stored row and whether it was created by this call.
 */
export const insert = (sql: Sql, t: NewTask) =>
  Effect.gen(function* () {
    const created = yield* sql.exec(
      `INSERT INTO alchemy_agent_inbox
         (task_id, task, input, status, attempts, max_attempts, run_at, error, causation, created_at, settled_at)
       VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (task_id) DO NOTHING
       RETURNING seq`,
      t.taskId,
      t.task,
      JSON.stringify(t.input ?? null),
      t.failure ? "failed" : "pending",
      t.maxAttempts,
      t.now,
      t.failure ? JSON.stringify(t.failure) : null,
      t.causation,
      t.now,
      t.failure ? t.now : null,
    );
    const isNew = (yield* created.toArray()).length > 0;
    const row = yield* get(sql, t.taskId);
    return { row: row!, isNew };
  });

/** Rows left `running` by an execution that never finished (eviction, CPU limit, crash). */
export const running = (sql: Sql) =>
  Effect.flatMap(
    sql.exec<Row>("SELECT * FROM alchemy_agent_inbox WHERE status = 'running' ORDER BY seq"),
    (c) => c.toArray(),
  );

/** The oldest task that is due. FIFO by arrival among due tasks. */
export const nextDue = (sql: Sql, now: number) =>
  one(
    sql,
    "SELECT * FROM alchemy_agent_inbox WHERE status = 'pending' AND run_at <= ? ORDER BY seq LIMIT 1",
    now,
  );

/** When the drain next has work: the earliest `run_at` of a pending task. */
export const nextRunAt = (sql: Sql) =>
  Effect.flatMap(
    sql.exec<{ at: number | null }>(
      "SELECT MIN(run_at) AS at FROM alchemy_agent_inbox WHERE status = 'pending'",
    ),
    (c) => c.one(),
  ).pipe(Effect.map((r) => r.at ?? undefined));

export const markRunning = (sql: Sql, seq: number, now: number) =>
  sql
    .exec(
      "UPDATE alchemy_agent_inbox SET status = 'running', attempts = attempts + 1, started_at = ? WHERE seq = ? AND status = 'pending'",
      now,
      seq,
    )
    .pipe(Effect.asVoid);

export const complete = (sql: Sql, seq: number, result: unknown, now: number) =>
  sql
    .exec(
      "UPDATE alchemy_agent_inbox SET status = 'completed', result = ?, settled_at = ? WHERE seq = ? AND status = 'running'",
      JSON.stringify(result ?? null),
      now,
      seq,
    )
    .pipe(Effect.asVoid);

export const fail = (sql: Sql, seq: number, error: StoredFailure, now: number) =>
  sql
    .exec(
      "UPDATE alchemy_agent_inbox SET status = 'failed', error = ?, settled_at = ? WHERE seq = ? AND status IN ('running', 'pending')",
      JSON.stringify(error),
      now,
      seq,
    )
    .pipe(Effect.asVoid);

export const retryLater = (sql: Sql, seq: number, error: StoredFailure, runAt: number) =>
  sql
    .exec(
      "UPDATE alchemy_agent_inbox SET status = 'pending', error = ?, run_at = ? WHERE seq = ? AND status = 'running'",
      JSON.stringify(error),
      runAt,
      seq,
    )
    .pipe(Effect.asVoid);

/**
 * Advances and returns the drain cursor: a strictly increasing number the drain passes as
 * `progress` when it reschedules itself (without it, Alchemy treats each reschedule as a
 * retry and backs off; see docs/spike/0.3-durable-callbacks.md E9).
 */
export const advanceCursor = (sql: Sql, key: "drain_cursor" | "step_cursor" | "outbox_cursor" = "drain_cursor") =>
  Effect.flatMap(
    sql.exec<{ value: number }>(
      "INSERT INTO alchemy_agent_meta (key, value) VALUES (?, 1) ON CONFLICT (key) DO UPDATE SET value = value + 1 RETURNING value",
      key,
    ),
    (c) => c.one(),
  ).pipe(Effect.map((r) => r.value));

/** The handler reached an unfinished turn or call: the task waits. The suspended run isn't an attempt. */
export const suspend = (sql: Sql, seq: number) =>
  sql
    .exec(
      "UPDATE alchemy_agent_inbox SET status = 'waiting', attempts = attempts - 1 WHERE seq = ? AND status = 'running'",
      seq,
    )
    .pipe(Effect.asVoid);

/** What a waiting task waited for settled: deliver it again, now. Returns whether it was waiting. */
export const resume = (sql: Sql, taskId: string, now: number) =>
  Effect.flatMap(
    sql.exec<{ seq: number }>(
      "UPDATE alchemy_agent_inbox SET status = 'pending', run_at = ? WHERE task_id = ? AND status = 'waiting' RETURNING seq",
      now,
      taskId,
    ),
    (c) => c.toArray(),
  ).pipe(Effect.map((rows) => rows.length > 0));

/** Increments a durable counter (used by `alchemy-agent/testing`). */
export const count = (sql: Sql, name: string) =>
  Effect.flatMap(
    sql.exec<{ value: number }>(
      "INSERT INTO alchemy_agent_counters (name, value) VALUES (?, 1) ON CONFLICT (name) DO UPDATE SET value = value + 1 RETURNING value",
      name,
    ),
    (c) => c.one(),
  ).pipe(Effect.map((r) => r.value));

export const isSettled = (row: Row) => row.status === "completed" || row.status === "failed";

/**
 * Settles a `waiting` task from outside its handler (M7: a person's answer). Returns whether it
 * settled: a task that already settled, or isn't waiting, is left alone.
 */
export const resolveWaiting = (
  sql: Sql,
  taskId: string,
  outcome: { readonly status: "completed"; readonly result: unknown } | { readonly status: "failed"; readonly error: StoredFailure },
  now: number,
) =>
  Effect.flatMap(
    outcome.status === "completed"
      ? sql.exec<{ seq: number }>(
          "UPDATE alchemy_agent_inbox SET status = 'completed', result = ?, settled_at = ? WHERE task_id = ? AND status = 'waiting' RETURNING seq",
          JSON.stringify(outcome.result ?? null),
          now,
          taskId,
        )
      : sql.exec<{ seq: number }>(
          "UPDATE alchemy_agent_inbox SET status = 'failed', error = ?, settled_at = ? WHERE task_id = ? AND status = 'waiting' RETURNING seq",
          JSON.stringify(outcome.error),
          now,
          taskId,
        ),
    (c) => c.toArray(),
  ).pipe(Effect.map((rows) => rows.length > 0));

/**
 * Settles a task that hasn't settled yet as failed with `error` (a cancellation). Works on any
 * unsettled status: a `running` handler's own outcome is then discarded, because every write
 * that records an outcome requires `running`. Returns whether this call settled it.
 */
export const cancel = (sql: Sql, taskId: string, error: StoredFailure, now: number) =>
  Effect.flatMap(
    sql.exec<{ seq: number }>(
      "UPDATE alchemy_agent_inbox SET status = 'failed', error = ?, settled_at = ? WHERE task_id = ? AND status IN ('pending', 'running', 'waiting') RETURNING seq",
      JSON.stringify(error),
      now,
      taskId,
    ),
    (c) => c.toArray(),
  ).pipe(Effect.map((rows) => rows.length > 0));

/** Up to `limit` tasks that settled before `before`, oldest first (retention). */
export const settledBefore = (sql: Sql, before: number, limit: number) =>
  Effect.flatMap(
    sql.exec<{ task_id: string }>(
      "SELECT task_id FROM alchemy_agent_inbox WHERE settled_at IS NOT NULL AND settled_at < ? AND status IN ('completed', 'failed') ORDER BY settled_at LIMIT ?",
      before,
      limit,
    ),
    (c) => c.toArray(),
  ).pipe(Effect.map((rows) => rows.map((r) => r.task_id)));

/** Removes a settled task's row. Its task id can then be recorded again, as a new task. */
export const remove = (sql: Sql, taskId: string) =>
  sql.exec("DELETE FROM alchemy_agent_inbox WHERE task_id = ? AND status IN ('completed', 'failed')", taskId).pipe(Effect.asVoid);

/** Tasks in arrival order, optionally only some statuses (inspection, M7). */
export const list = (
  sql: Sql,
  options: { readonly statuses?: ReadonlyArray<string>; readonly taskId?: string; readonly after?: number; readonly limit?: number },
) => {
  const statuses = options.statuses ?? [];
  const where = [
    statuses.length > 0 ? `AND status IN (${statuses.map(() => "?").join(", ")})` : "",
    options.taskId !== undefined ? "AND task_id = ?" : "",
  ].join(" ");
  const bindings = [...statuses, ...(options.taskId !== undefined ? [options.taskId] : [])];
  return Effect.flatMap(
    sql.exec<Row>(`SELECT * FROM alchemy_agent_inbox WHERE seq > ? ${where} ORDER BY seq LIMIT ?`, options.after ?? 0, ...bindings, Math.min(options.limit ?? 100, 500)),
    (c) => c.toArray(),
  ).pipe(Effect.map((rows) => rows.map((r) => ({ seq: r.seq, ...toRecord(r), input: JSON.parse(r.input) as unknown }))));
};
