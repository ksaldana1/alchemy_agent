// The agent Durable Object: inbox, drain loop, and the RPC surface references call.
// See docs/ARCHITECTURE.md ("The inbox") for the data flow.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  AgentContext,
  CurrentExecution,
  reachKey,
  type AgentClass,
  type AnyTaskDef,
  type AnyTasks,
  type RetryPolicy,
  type TaskSchema,
} from "./core.ts";
import * as Inbox from "./inbox.ts";
import { bindingTransport, type AgentRpc, type Namespace } from "./ref.ts";

const DEFAULT_RETRY = { maxAttempts: 3, delay: "1 second", maxDelay: "5 minutes" } as const;
/** Tasks run per drain pass before it reschedules itself, so one pass stays short. */
const DRAIN_BATCH = 10;
const DRAIN_CALLBACK = "alchemy-agent/drain";

class InvalidInput extends Data.TaggedError("InvalidInput")<{ readonly message: string }> {}

type Outcome =
  | { readonly _tag: "Completed"; readonly result: unknown }
  | { readonly _tag: "Failed"; readonly permanent: boolean; readonly error: Inbox.StoredFailure };

const failureOf = (error: unknown): Inbox.StoredFailure =>
  typeof error === "object" && error !== null && "_tag" in error
    ? {
        _tag: String((error as { _tag: unknown })._tag),
        message: String((error as { message?: unknown }).message ?? ""),
      }
    : { _tag: "Defect", message: error instanceof Error ? error.message : String(error) };

export interface AgentConfig {
  readonly name: string;
  readonly tasks: AnyTasks;
  readonly retry: RetryPolicy | undefined;
  readonly uses: ReadonlyArray<AgentClass>;
}

/** The outer (construction) Effect for `Cloudflare.DurableObject<Self>()(name, ...)`. */
export const agentObject = (config: AgentConfig) => {
  const { name, tasks, uses } = config;
  const policyOf = (task: string) => {
    const p = { ...DEFAULT_RETRY, ...config.retry, ...tasks[task]?.retry };
    return {
      maxAttempts: p.maxAttempts,
      delayMs: Duration.toMillis(Duration.fromInputUnsafe(p.delay)),
      maxDelayMs: Duration.toMillis(Duration.fromInputUnsafe(p.maxDelay)),
    };
  };
  const backoff = (task: string, attempts: number) => {
    const p = policyOf(task);
    return Math.min(p.maxDelayMs, p.delayMs * 2 ** Math.max(0, attempts - 1));
  };

  return Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    // Construction phase: bind every agent this one uses (Alchemy only allows bindings here).
    const namespaces: Array<readonly [AgentClass, Namespace]> = [];
    for (const other of uses) {
      namespaces.push([other, (yield* (other as unknown as Effect.Effect<Namespace>)) as Namespace]);
    }

    return Effect.gen(function* () {
      const sql = state.storage.sql as unknown as Inbox.Sql;
      const transaction = <A>(effect: Effect.Effect<A, never, RuntimeContext>) =>
        state.storage.transaction(effect) as Effect.Effect<A, never, RuntimeContext>;
      yield* Inbox.migrate(sql, transaction);
      const agentName = state.id.name ?? state.id.toString();
      const now = () => Date.now();

      // In-memory waiters for `wait`: released after a task's settling transaction commits.
      // Lost on eviction, which is fine: `wait` callers loop and re-read the record.
      const waiters = new Map<string, Array<Deferred.Deferred<void>>>();
      const release = (taskId: string) =>
        Effect.suspend(() => {
          const ds = waiters.get(taskId) ?? [];
          waiters.delete(taskId);
          return Effect.forEach(ds, (d) => Deferred.succeed(d, undefined), { discard: true });
        });

      const execute = (row: Inbox.InboxRow, attempt: number): Effect.Effect<Outcome, never, RuntimeContext> => {
        const def: AnyTaskDef | undefined = tasks[row.task];
        if (def === undefined) {
          return Effect.succeed({
            _tag: "Failed",
            permanent: true,
            error: { _tag: "UnknownTaskError", message: `No task "${row.task}"` },
          });
        }
        const causation = `${name}/${agentName}/${row.task_id}`;
        let services = Context.make(AgentContext, {
          agent: { type: name, name: agentName },
          taskId: row.task_id,
          attempt,
        });
        for (const [other, namespace] of namespaces) {
          services = Context.add(services, reachKey(other.definition.name), {
            transport: (n: string) => bindingTransport(namespace, n, causation),
          });
        }

        return Effect.gen(function* () {
          const input = yield* Schema.decodeUnknownEffect(def.input as TaskSchema)(JSON.parse(row.input)).pipe(
            Effect.mapError((issue) => new InvalidInput({ message: issue.message })),
          );
          const value = yield* (def.handler(input) as Effect.Effect<unknown, unknown, never>).pipe(
            Effect.provideContext(services as Context.Context<never>),
            Effect.provideService(CurrentExecution, { abort: (reason) => state.raw.abort(reason) }),
            Effect.annotateLogs({ agent: `${name}/${agentName}`, taskId: row.task_id, attempt }),
          );
          return def.output === undefined
            ? (value ?? null)
            : yield* Schema.encodeUnknownEffect(def.output as TaskSchema)(value).pipe(Effect.orDie);
        }).pipe(
          Effect.map((result): Outcome => ({ _tag: "Completed", result })),
          Effect.catchCause((cause) => {
            const [reason] = cause.reasons;
            if (reason?._tag === "Fail") {
              if (reason.error instanceof InvalidInput) {
                return Effect.succeed<Outcome>({
                  _tag: "Failed",
                  permanent: true,
                  error: { _tag: "TaskInputError", message: reason.error.message },
                });
              }
              const error = failureOf(reason.error);
              return Effect.succeed<Outcome>({
                _tag: "Failed",
                permanent: error._tag === "PermanentError",
                error,
              });
            }
            return Effect.succeed<Outcome>({
              _tag: "Failed",
              permanent: false,
              error: {
                _tag: "Defect",
                message: reason?._tag === "Die" ? failureOf(reason.defect).message : "the handler was interrupted",
              },
            });
          }),
        );
      };

      // A failed attempt either settles the task or puts it back with a backoff.
      const afterFailure = (row: Inbox.InboxRow, attempts: number, error: Inbox.StoredFailure, permanent: boolean) =>
        permanent || attempts >= row.max_attempts
          ? Inbox.fail(sql, row.seq, error, now())
          : Inbox.retryLater(sql, row.seq, error, now() + backoff(row.task, attempts));

      let drain: Alchemy.Callback<{ readonly v: 1 }>;
      const scheduleDrain = (at: number, progress?: number) =>
        drain
          .schedule("drain", {
            at: Math.max(at, now()),
            payload: { v: 1 },
            ...(progress === undefined ? {} : { progress }),
          })
          .pipe(Effect.orDie);

      const drainPass = Effect.gen(function* () {
        // The drain is single-flight per Durable Object, so a `running` row at the start of a
        // pass belongs to an execution that ended without settling it: a lost attempt.
        for (const row of yield* Inbox.running(sql)) {
          yield* transaction(
            afterFailure(row, row.attempts, { _tag: "ExecutionLost", message: "the execution running this task ended" }, false),
          );
          yield* release(row.task_id);
        }
        let processed = 0;
        while (processed < DRAIN_BATCH) {
          const row = yield* Inbox.nextDue(sql, now());
          if (row === undefined) break;
          const attempt = row.attempts + 1;
          // Persist `running` before the handler runs, so a crash counts as an attempt.
          yield* transaction(Inbox.markRunning(sql, row.seq, now()));
          const result = yield* execute(row, attempt);
          yield* transaction(
            result._tag === "Completed"
              ? Inbox.complete(sql, row.seq, result.result, now())
              : afterFailure(row, attempt, result.error, result.permanent),
          );
          yield* release(row.task_id);
          processed++;
        }
        const next = yield* Inbox.nextRunAt(sql);
        if (next !== undefined) {
          // `progress` must strictly increase, or Alchemy counts the reschedule as a retry.
          yield* scheduleDrain(next, yield* Inbox.advanceCursor(sql));
        }
      });

      drain = yield* Alchemy.makeCallback(DRAIN_CALLBACK, (_: { readonly v: 1 }) => drainPass, {
        retry: { delay: "1 second" },
      });

      const record = (taskId: string) =>
        Inbox.get(sql, taskId).pipe(Effect.map((row) => (row === undefined ? null : Inbox.toRecord(row))));

      const rpc: AgentRpc = {
        enqueue: (taskId, task, input, options) =>
          transaction(
            Effect.gen(function* () {
              const def = tasks[task];
              // Unknown tasks and invalid input are recorded as failed, not thrown.
              const failure: Inbox.StoredFailure | undefined =
                def === undefined
                  ? { _tag: "UnknownTaskError", message: `No task "${task}"` }
                  : yield* Schema.decodeUnknownEffect(def.input as TaskSchema)(input).pipe(
                      Effect.as(undefined),
                      Effect.catch((issue) => Effect.succeed({ _tag: "TaskInputError", message: issue.message })),
                    );
              const { row, isNew } = yield* Inbox.insert(sql, {
                taskId,
                task,
                input,
                maxAttempts: policyOf(task).maxAttempts,
                causation: options.causation,
                now: now(),
                ...(failure ? { failure } : {}),
              });
              // Scheduled in the same transaction as the write (D4).
              if (isNew && !failure) yield* scheduleDrain(now());
              return Inbox.toRecord(row);
            }),
          ),
        get: record,
        wait: (taskId, timeoutMs) =>
          Effect.gen(function* () {
            const current = yield* record(taskId);
            if (current === null || current.status === "completed" || current.status === "failed") return current;
            const d = yield* Deferred.make<void>();
            waiters.set(taskId, [...(waiters.get(taskId) ?? []), d]);
            yield* Deferred.await(d).pipe(Effect.timeoutOption(Duration.millis(timeoutMs)));
            waiters.set(taskId, (waiters.get(taskId) ?? []).filter((x) => x !== d));
            return yield* record(taskId);
          }),
      };
      return rpc;
    });
  });
};
