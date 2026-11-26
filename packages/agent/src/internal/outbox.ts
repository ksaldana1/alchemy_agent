// Messages between agents (M3). A client call made inside an agent becomes an outbox row,
// committed with the outcome of the execution that made it, and delivered by a durable callback.
// A waiting call suspends its owner (a task, or a turn when a tool made it). The recipient keeps
// a subscriber per waiting caller and, when the task settles, queues a reply in its own outbox.
// See docs/ARCHITECTURE.md ("Messages between agents").
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { callAgent } from "./client.ts";
import { AgentErrorReason, Calls, makeSuspension, suspend, Unreachable, type Address, type CallRequest, type Suspension } from "./core.ts";
import * as Inbox from "./inbox.ts";
import { WORKFLOW_PREFIX } from "./workflow.ts";

/** Deliveries per pass before the callback reschedules itself. */
const BATCH = 20;
/** Delivery attempts before a call fails with `Unreachable` (about 17 minutes with the backoff). */
const MAX_ATTEMPTS = 10;
const backoffMs = (attempts: number) => Math.min(300_000, 1_000 * 2 ** Math.max(0, attempts - 1));

/** What waits on a call: a task's handler, or one of a turn's tool calls. */
export interface Owner {
  readonly kind: "task" | "turn";
  /** The task id or turn id resumed when the answer arrives. */
  readonly id: string;
  /** Row ids are `<prefix>#c<n>`: the n-th call of this execution. */
  readonly prefix: string;
  /** The task the calls are made on behalf of (`Type/name/taskId`). */
  readonly causation: string;
}

type Row = {
  id: string;
  kind: "call" | "reply";
  owner_kind: "task" | "turn" | null;
  owner_id: string | null;
  target_type: string;
  target_name: string;
  task_id: string;
  tag: string | null;
  payload: string | null;
  reply_id: string | null;
  wait: number;
  causation: string | null;
  status: "pending" | "sent" | "settled" | "failed";
  record: string | null;
  attempts: number;
  run_at: number;
  created_at: number;
};

export interface OutboxDeps {
  readonly sql: Inbox.Sql;
  readonly transaction: <A>(effect: Effect.Effect<A, never, RuntimeContext>) => Effect.Effect<A, never, RuntimeContext>;
  readonly now: () => number;
  readonly self: Address;
  /** Schedules the delivery callback (inside the caller's transaction). */
  readonly schedule: (at: number, progress: number) => Effect.Effect<void, never, RuntimeContext>;
  /** Resumes what waited on a call (inside the transaction that recorded the answer). */
  readonly wake: (owner: { readonly kind: "task" | "turn"; readonly id: string }) => Effect.Effect<void, never, RuntimeContext>;
  /** Delivers a reply to a waiting workflow run (`workflow:<Name>` / executionId / deferred). */
  readonly signalWorkflow: (target: Address, deferredName: string, record: Inbox.TaskRecord) => Effect.Effect<void, unknown>;
}

const encodeReason = Schema.encodeSync(Schema.toCodecJson(AgentErrorReason as Schema.Codec<unknown, unknown, never, never>));

export const makeOutbox = (deps: OutboxDeps) => {
  const { sql, transaction, now, self } = deps;
  const all = <T extends Record<string, unknown>>(query: string, ...bindings: Array<unknown>) =>
    Effect.flatMap(sql.exec<T & Record<string, string | number | null>>(query, ...bindings), (c) => c.toArray()) as Effect.Effect<
      Array<T>,
      never,
      RuntimeContext
    >;
  const get = (id: string) => Effect.map(all<Row>("SELECT * FROM alchemy_agent_outbox WHERE id = ?", id), (rows) => rows[0]);

  const scheduleDelivery = Effect.fnUntraced(function* (at: number) {
    yield* deps.schedule(at, yield* Inbox.advanceCursor(sql, "outbox_cursor"));
  });

  const wakeOwner = (row: Row) =>
    row.owner_kind === null || row.owner_id === null ? Effect.void : deps.wake({ kind: row.owner_kind, id: row.owner_id });

  /** Records a waiting call's answer and resumes its owner, once. */
  const settleCall = Effect.fnUntraced(function* (id: string, record: Inbox.TaskRecord) {
    const updated = yield* all<Row>(
      "UPDATE alchemy_agent_outbox SET status = 'settled', record = ? WHERE id = ? AND status IN ('pending', 'sent') RETURNING *",
      JSON.stringify(record),
      id,
    );
    for (const row of updated) yield* wakeOwner(row);
  });

  // ---- Calls made by one execution -------------------------------------------------------

  /**
   * The `Calls` service for one execution of a handler or tool, `flush`, which records the new
   * calls, and the execution's `suspension` (set when a waiting call suspends it). Run `flush` in the transaction that records the execution's outcome: a failed
   * execution's calls are dropped, and the rerun makes them again.
   */
  const calls = (owner: Owner, suspension: Suspension = makeSuspension()) => {
    let index = 0;
    const buffered: Array<CallRequest & { readonly id: string; readonly taskId: string }> = [];
    const service = Calls.of({
      call: (request) =>
        Effect.gen(function* () {
          const n = index++;
          const id = `${owner.prefix}#c${n}`;
          const existing = yield* get(id);
          if (existing !== undefined) {
            if (
              existing.target_type !== request.agent.type ||
              existing.target_name !== request.agent.name ||
              existing.tag !== request.tag ||
              (existing.wait === 1) !== request.wait
            ) {
              return yield* Effect.die(
                new Error(
                  `call #${n} was ${existing.target_type}/${existing.target_name}.${existing.tag} on an earlier run, and is ` +
                    `${request.agent.type}/${request.agent.name}.${request.tag} now. A handler must make the same calls in the same order.`,
                ),
              );
            }
            if (!request.wait) return undefined;
            if (existing.status === "settled") return JSON.parse(existing.record!) as Inbox.TaskRecord;
            return yield* suspend(suspension, id);
          }
          buffered.push({ ...request, id, taskId: request.taskId ?? `${self.type}/${self.name}/${id}` });
          if (!request.wait) return undefined;
          // Calls started alongside this one (`Effect.all`) get to record theirs before it suspends.
          yield* Effect.yieldNow;
          return yield* suspend(suspension, id);
        }) as Effect.Effect<Inbox.TaskRecord | undefined, never, RuntimeContext> as Effect.Effect<Inbox.TaskRecord | undefined>,
    });
    const flush = Effect.gen(function* () {
      if (buffered.length === 0) return;
      for (const c of buffered) {
        yield* sql.exec(
          `INSERT INTO alchemy_agent_outbox
             (id, kind, owner_kind, owner_id, target_type, target_name, task_id, tag, payload, wait, causation, status, run_at, created_at)
           VALUES (?, 'call', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
           ON CONFLICT (id) DO NOTHING`,
          c.id,
          c.wait ? owner.kind : null,
          c.wait ? owner.id : null,
          c.agent.type,
          c.agent.name,
          c.taskId,
          c.tag,
          JSON.stringify(c.payload ?? null),
          c.wait ? 1 : 0,
          owner.causation,
          now(),
          now(),
        );
      }
      buffered.length = 0;
      yield* scheduleDelivery(now());
    });
    return { service, flush, suspension };
  };

  // ---- Delivery (the durable callback) --------------------------------------------------

  const deliver = (row: Row) =>
    Effect.gen(function* () {
      const target: Address = { type: row.target_type, name: row.target_name };
      if (row.kind === "reply" && row.target_type.startsWith(WORKFLOW_PREFIX)) {
        yield* deps.signalWorkflow(target, row.reply_id!, JSON.parse(row.record!));
        return undefined;
      }
      if (row.kind === "reply") {
        yield* callAgent(target, (stub) => stub.reply(row.reply_id!, JSON.parse(row.record!)));
        return undefined;
      }
      const recorded = yield* callAgent(target, (stub) =>
        stub.enqueue(row.task_id, row.tag!, JSON.parse(row.payload!), { causation: row.causation }),
      );
      yield* Effect.logInfo("message.sent").pipe(
        Effect.annotateLogs({ to: `${target.type}/${target.name}`, task: row.tag, taskId: row.task_id, causation: row.causation }),
      );
      if (row.wait === 0) return undefined;
      // Already settled (or refused: an `InputConflict` record isn't stored, so there's nothing to subscribe to).
      if (recorded.status === "completed" || recorded.status === "failed") return recorded;
      // A recipient still on pre-M3 code has no `subscribe`: this fails and is retried.
      const record = yield* callAgent(target, (stub) => stub.subscribe(row.task_id, { type: self.type, name: self.name, id: row.id }));
      if (record === null) return yield* Effect.fail(new Error(`${target.type}/${target.name} has no task ${row.task_id}`));
      return record;
    });

  const failDelivery = Effect.fnUntraced(function* (row: Row, error: unknown) {
    const attempts = row.attempts + 1;
    const message = error instanceof Error ? error.message : String((error as { message?: unknown })?.message ?? error);
    if (attempts < MAX_ATTEMPTS) {
      yield* sql.exec(
        "UPDATE alchemy_agent_outbox SET attempts = ?, run_at = ? WHERE id = ? AND status = 'pending'",
        attempts,
        now() + backoffMs(attempts),
        row.id,
      );
      return;
    }
    yield* Effect.logError("message.undeliverable").pipe(Effect.annotateLogs({ id: row.id, to: `${row.target_type}/${row.target_name}`, error: message }));
    if (row.kind === "call" && row.wait === 1) {
      // The waiting caller gets an AgentError(Unreachable) instead of waiting forever.
      return yield* settleCall(row.id, {
        taskId: row.task_id,
        task: row.tag!,
        status: "failed",
        attempts: 0,
        maxAttempts: 0,
        error: { kind: "agent", reason: encodeReason(new Unreachable({ message: `couldn't deliver to ${row.target_type}/${row.target_name}: ${message}` })) },
        causation: row.causation,
        createdAt: row.created_at,
        settledAt: now(),
      });
    }
    yield* sql.exec("UPDATE alchemy_agent_outbox SET status = 'failed', attempts = ? WHERE id = ? AND status = 'pending'", attempts, row.id);
  });

  /** One delivery pass. Never fails: a failed delivery is retried with a backoff. */
  const deliveryPass = Effect.gen(function* () {
    const due = yield* all<Row>(
      "SELECT * FROM alchemy_agent_outbox WHERE status = 'pending' AND run_at <= ? ORDER BY run_at, rowid LIMIT ?",
      now(),
      BATCH,
    );
    for (const row of due) {
      const result = yield* deliver(row).pipe(Effect.result);
      yield* transaction(
        result._tag === "Failure"
          ? failDelivery(row, result.failure)
          : result.success !== undefined && (result.success.status === "completed" || result.success.status === "failed")
            ? settleCall(row.id, result.success) // already settled: no reply will follow
            : sql.exec("UPDATE alchemy_agent_outbox SET status = 'sent' WHERE id = ? AND status = 'pending'", row.id).pipe(Effect.asVoid),
      );
    }
    const [next] = yield* all<{ at: number | null }>("SELECT MIN(run_at) AS at FROM alchemy_agent_outbox WHERE status = 'pending'");
    if (next?.at !== null && next?.at !== undefined) yield* transaction(scheduleDelivery(next.at));
  });

  // ---- The recipient's side --------------------------------------------------------------

  /** RPC `subscribe`: remember who waits for a task, unless it has already settled. */
  const subscribe = (taskId: string, replyTo: { readonly type: string; readonly name: string; readonly id: string }) =>
    transaction(
      Effect.gen(function* () {
        const row = yield* Inbox.get(sql, taskId);
        if (row === undefined) return null;
        if (!Inbox.isSettled(row)) {
          yield* sql.exec(
            "INSERT INTO alchemy_agent_subscribers (task_id, reply_type, reply_name, reply_id) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
            taskId,
            replyTo.type,
            replyTo.name,
            replyTo.id,
          );
        }
        return Inbox.toRecord(row);
      }),
    );

  /** In the transaction that settles a task: queue a reply to everyone waiting for it. */
  const notify = Effect.fnUntraced(function* (taskId: string) {
    const row = yield* Inbox.get(sql, taskId);
    if (row === undefined || !Inbox.isSettled(row)) return;
    const subscribers = yield* all<{ reply_type: string; reply_name: string; reply_id: string }>(
      "DELETE FROM alchemy_agent_subscribers WHERE task_id = ? RETURNING reply_type, reply_name, reply_id",
      taskId,
    );
    if (subscribers.length === 0) return;
    const record = JSON.stringify(Inbox.toRecord(row));
    for (const s of subscribers) {
      yield* sql.exec(
        `INSERT INTO alchemy_agent_outbox (id, kind, target_type, target_name, task_id, reply_id, status, record, run_at, created_at)
         VALUES (?, 'reply', ?, ?, ?, ?, 'pending', ?, ?, ?)
         ON CONFLICT (id) DO NOTHING`,
        `reply:${taskId}->${s.reply_type}/${s.reply_name}/${s.reply_id}`,
        s.reply_type,
        s.reply_name,
        taskId,
        s.reply_id,
        record,
        now(),
        now(),
      );
    }
    yield* scheduleDelivery(now());
  });

  /** RPC `reply`: the answer to one of this agent's waiting calls. */
  const reply = (id: string, record: Inbox.TaskRecord) => transaction(settleCall(id, record));

  // ---- Cancellation and retention --------------------------------------------------------

  // Every call a task makes, from its handler (`<taskId>#c<n>`) or from a tool in one of its
  // turns (`<taskId>#<i>/<toolCallId>#c<n>`), has an id that starts with `<taskId>#`.
  const ownedBy = "substr(id, 1, ?) = ?";
  const prefixOf = (taskId: string) => [`${taskId}#`.length, `${taskId}#`] as const;

  /** In the transaction that cancels a task: its calls not yet delivered are never sent. */
  const dropCalls = (taskId: string) =>
    sql
      .exec(`UPDATE alchemy_agent_outbox SET status = 'failed' WHERE kind = 'call' AND status = 'pending' AND ${ownedBy}`, ...prefixOf(taskId))
      .pipe(Effect.asVoid);

  /** Retention: removes the calls a pruned task made. */
  const removeCalls = (taskId: string) =>
    sql.exec(`DELETE FROM alchemy_agent_outbox WHERE kind = 'call' AND ${ownedBy}`, ...prefixOf(taskId)).pipe(Effect.asVoid);

  /** Retention: removes replies that were delivered (or given up on) before `before`. */
  const removeReplies = (before: number) =>
    sql
      .exec("DELETE FROM alchemy_agent_outbox WHERE kind = 'reply' AND status IN ('sent', 'failed') AND created_at < ?", before)
      .pipe(Effect.asVoid);

  return { calls, deliveryPass, subscribe, notify, reply, dropCalls, removeCalls, removeReplies };
};

export type Outbox = ReturnType<typeof makeOutbox>;
