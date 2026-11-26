// Agent definitions, the Durable Object that runs them, and typed clients.
//
// One agent definition is one Durable Object class (D2). An agent is one instance, addressed by
// name. Its inbox is a SQLite table inside that instance (D3), drained by a durable callback
// scheduled in the same transaction as the write that needs it (D4).
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Inbox from "./internal/inbox.ts";

export type { StoredFailure, TaskRecord, TaskStatus } from "./internal/inbox.ts";

// ---------------------------------------------------------------------------
// Services and errors agent code sees
// ---------------------------------------------------------------------------

export class AgentContext extends Context.Service<
  AgentContext,
  {
    /** The agent running this task: its definition (`type`) and its name. */
    readonly agent: { readonly type: string; readonly name: string };
    readonly taskId: string;
    /** 1 on the first delivery, 2 on the first redelivery, and so on. */
    readonly attempt: number;
  }
>()("alchemy-agent/AgentContext") {}

/** Fail with this to settle the task right away, without further attempts. */
export class PermanentError extends Schema.TaggedError<PermanentError>()("PermanentError", {
  message: Schema.String,
}) {}

/** `handle.result()` fails with this when the task settled as failed. */
export class TaskFailed<E = unknown> extends Data.TaggedError("TaskFailed")<{
  readonly taskId: string;
  readonly task: string;
  readonly attempts: number;
  readonly error: TaskFailure<E>;
}> {}

/** `handle.result()` / `handle.status` for a task id this agent has never recorded. */
export class TaskNotFound extends Data.TaggedError("TaskNotFound")<{
  readonly taskId: string;
}> {}

/**
 * A failure as stored and as it crosses the wire: the tag and message survive, class identity
 * does not. Besides the handler's own errors, the framework settles tasks with:
 * - `TaskInputError`: the input didn't decode against the task's schema
 * - `UnknownTaskError`: the agent has no task by that name
 * - `ExecutionLost`: the execution running the task disappeared (eviction, CPU limit, crash)
 * - `Defect`: the handler died (a thrown error or `Effect.die`)
 */
export type TaskFailure<E> =
  | (E extends { readonly _tag: infer Tag extends string }
      ? { readonly _tag: Tag; readonly message: string }
      : never)
  | {
      readonly _tag: "TaskInputError" | "UnknownTaskError" | "ExecutionLost" | "Defect";
      readonly message: string;
    };

// ---------------------------------------------------------------------------
// Task definitions
// ---------------------------------------------------------------------------

/** Task schemas run on both sides of the wire, so they can't need services. */
export type TaskSchema = Schema.Codec<any, any, never, never>;

/** What a task may return when it has no `output` schema: plain JSON. */
export type Json =
  | string
  | number
  | boolean
  | null
  | ReadonlyArray<Json>
  | { readonly [key: string]: Json };

export interface RetryPolicy {
  /** Deliveries before the task fails. Default 3. */
  readonly maxAttempts?: number;
  /** Delay before the first redelivery; doubles each time. Default 1 second. */
  readonly delay?: Duration.Input;
  /** Upper bound on the delay. Default 5 minutes. */
  readonly maxDelay?: Duration.Input;
}

export interface TaskDef<In extends TaskSchema, Out extends TaskSchema | undefined, A, E, R> {
  readonly input: In;
  readonly output: Out;
  readonly retry: RetryPolicy | undefined;
  readonly handler: (input: In["Type"]) => Effect.Effect<A, E, R>;
}

// `any` for the input schema: `TaskDef` is invariant in it (it's also the handler's parameter).
export type AnyTaskDef = TaskDef<any, any, any, any, any>;
type AnyTasks = Record<string, AnyTaskDef>;

/** A task with an `output` schema: the handler returns its `Type`; its `Encoded` is stored. */
export function task<In extends TaskSchema, Out extends TaskSchema, E = never, R = never>(
  spec: { readonly input: In; readonly output: Out; readonly retry?: RetryPolicy },
  handler: (input: In["Type"]) => Effect.Effect<Out["Type"], E, R>,
): TaskDef<In, Out, Out["Type"], E, R>;
/** A task whose output type is inferred from the handler. It must be plain JSON. */
export function task<In extends TaskSchema, A, E = never, R = never>(
  spec: { readonly input: In; readonly retry?: RetryPolicy },
  handler: (input: In["Type"]) => Effect.Effect<A, E, R>,
): TaskDef<In, undefined, A, E, R>;
export function task(
  spec: { readonly input: TaskSchema; readonly output?: TaskSchema; readonly retry?: RetryPolicy },
  handler: (input: unknown) => Effect.Effect<unknown, unknown, unknown>,
): AnyTaskDef {
  return { input: spec.input, output: spec.output, retry: spec.retry, handler };
}

// ---------------------------------------------------------------------------
// Helper types. They read the static definition, never Alchemy's RPC types.
// ---------------------------------------------------------------------------

export interface AgentDefinition<Name extends string, Tasks extends AnyTasks> {
  readonly name: Name;
  readonly tasks: Tasks;
  readonly retry: RetryPolicy | undefined;
}

/** Anything carrying a definition: an agent class, or the definition itself. */
type HasDefinition = { readonly definition: AgentDefinition<string, AnyTasks> };
type DefOf<Ag> = Ag extends { readonly definition: infer D extends AgentDefinition<any, any> }
  ? D
  : never;
type TasksOf<Ag> = DefOf<Ag>["tasks"];

export type AgentType<Ag> = DefOf<Ag>["name"];
export type TaskNames<Ag> = keyof TasksOf<Ag> & string;
/** What a caller passes: the input schema's encoded (JSON) form. */
export type TaskInput<Ag, K extends TaskNames<Ag>> = TasksOf<Ag>[K]["input"]["Encoded"];
/** What the handler receives: the input schema's decoded form. */
export type TaskInputDecoded<Ag, K extends TaskNames<Ag>> = TasksOf<Ag>[K]["input"]["Type"];
export type TaskOutput<Ag, K extends TaskNames<Ag>> = OutputOf<TasksOf<Ag>[K]>;
export type TaskError<Ag, K extends TaskNames<Ag>> = ErrorOf<TasksOf<Ag>[K]>;

type OutputOf<T> = T extends TaskDef<any, infer Out, infer A, any, any>
  ? Out extends TaskSchema
    ? Out["Type"]
    : A
  : never;
type ErrorOf<T> = T extends TaskDef<any, any, any, infer E, any> ? E : never;
type ServicesOf<T> = T extends TaskDef<any, any, any, any, infer R> ? R : never;
type RawOutputOf<T> = T extends TaskDef<any, infer Out, infer A, any, any>
  ? Out extends TaskSchema
    ? Json
    : A
  : never;

/**
 * Compile-time checks on each task, reported against the task's name:
 * - its handler needs only `AgentContext` and the agents listed in `uses`
 * - without an `output` schema, it returns plain JSON (or nothing)
 */
type CheckTasks<Tasks extends AnyTasks, Allowed> = {
  [K in keyof Tasks]: [Exclude<ServicesOf<Tasks[K]>, Allowed>] extends [never]
    ? [RawOutputOf<Tasks[K]>] extends [Json | void | undefined]
      ? Tasks[K]
      : `Task "${K & string}" returns a value that isn't plain JSON; add an output schema`
    : `Task "${K & string}" needs a service the framework does not provide`;
};

// ---------------------------------------------------------------------------
// Clients: typed from the definition, over any transport (DO binding or HTTP)
// ---------------------------------------------------------------------------

export interface SendOptions {
  /** The task id, which doubles as the idempotency key. Default: a random UUID. */
  readonly taskId?: string;
}

export interface TaskHandle<Ag, K extends TaskNames<Ag>, E = never, R = never> {
  readonly taskId: string;
  readonly task: K;
  /** The stored record, as it is now. */
  readonly status: Effect.Effect<Inbox.TaskRecord, TaskNotFound | E, R>;
  /**
   * Waits until the task settles: its output if it completed, `TaskFailed` if it failed.
   * Polls with backoff and never gives up on its own; add `Effect.timeout` to bound it.
   */
  result(): Effect.Effect<TaskOutput<Ag, K>, TaskFailed<TaskError<Ag, K>> | TaskNotFound | E, R>;
}

export interface AgentClient<Ag, E = never, R = never> {
  readonly type: AgentType<Ag>;
  readonly name: string;
  /** Records a task in the agent's inbox and returns its handle right away. */
  send<K extends TaskNames<Ag>>(
    task: K,
    input: TaskInput<Ag, K>,
    options?: SendOptions,
  ): Effect.Effect<TaskHandle<Ag, K, E, R>, E, R>;
  /** A handle for a task sent earlier. */
  handle<K extends TaskNames<Ag>>(task: K, taskId: string): TaskHandle<Ag, K, E, R>;
}

/** How a client reaches one agent. Internal; `lookup`, `use`, and `Api.client` build them. */
export interface Transport<E, R> {
  readonly enqueue: (
    taskId: string,
    task: string,
    input: unknown,
  ) => Effect.Effect<Inbox.TaskRecord, E, R>;
  readonly get: (taskId: string) => Effect.Effect<Inbox.TaskRecord | null, E, R>;
}

// Exponential from 100 ms, capped at 2 s between polls.
const pollSchedule = Schedule.min([Schedule.exponential("100 millis"), Schedule.spaced("2 seconds")]);

export const makeClient = <Ag extends HasDefinition, E, R>(
  agent: Ag,
  name: string,
  transport: Transport<E, R>,
): AgentClient<Ag, E, R> => {
  const definition = agent.definition;
  const handle = <K extends TaskNames<Ag>>(task: K, taskId: string): TaskHandle<Ag, K, E, R> => {
    const status = Effect.flatMap(transport.get(taskId), (record) =>
      record === null ? Effect.fail(new TaskNotFound({ taskId })) : Effect.succeed(record),
    );
    return {
      taskId,
      task,
      status,
      result: () =>
        status.pipe(
          Effect.repeat({
            schedule: pollSchedule,
            until: (r) => r.status === "completed" || r.status === "failed",
          }),
          Effect.flatMap((record) => {
            if (record.status === "failed") {
              return Effect.fail(
                new TaskFailed<TaskError<Ag, K>>({
                  taskId,
                  task: record.task,
                  attempts: record.attempts,
                  error: (record.error ?? {
                    _tag: "Defect",
                    message: "failed without an error",
                  }) as TaskFailure<TaskError<Ag, K>>,
                }),
              );
            }
            const output = definition.tasks[record.task]?.output as TaskSchema | undefined;
            return output === undefined
              ? Effect.succeed(record.result as TaskOutput<Ag, K>)
              : Schema.decodeUnknownEffect(output)(record.result).pipe(
                  Effect.orDie,
                  Effect.map((value) => value as TaskOutput<Ag, K>),
                );
          }),
        ),
    };
  };
  return {
    type: definition.name as AgentType<Ag>,
    name,
    send: (task, input, options) =>
      Effect.suspend(() => {
        const taskId = options?.taskId ?? crypto.randomUUID();
        return transport
          .enqueue(taskId, task, input)
          .pipe(Effect.map((record) => handle(record.task as typeof task, record.taskId)));
      }),
    handle,
  };
};

// ---------------------------------------------------------------------------
// Agent references: one agent using another, and a Worker using an agent
// ---------------------------------------------------------------------------

/** The service a handler needs to reach agent `Type`. Only `uses` provides it. */
export interface AgentRef<Type extends string> {
  readonly _AgentRef: Type;
}

export interface AgentLookup<Ag> {
  readonly getByName: (name: string) => AgentClient<Ag>;
}

/** The Durable Object RPC surface. Framework-internal: callers use a client. */
export type AgentRpc = {
  enqueue: (
    taskId: string,
    task: string,
    input: unknown,
    options: { readonly causation: string | null },
  ) => Effect.Effect<Inbox.TaskRecord, never, RuntimeContext>;
  get: (taskId: string) => Effect.Effect<Inbox.TaskRecord | null, never, RuntimeContext>;
};

/** Any class made by `Agent.make`: a static definition plus Alchemy's binding Effect. */
export type AgentClass = HasDefinition & Effect.Effect<unknown, never, any>;

type Namespace = { readonly getByName: (name: string) => unknown };

const bindingTransport = (
  namespace: Namespace,
  name: string,
  causation: string | null,
): Transport<never, never> => {
  const stub = namespace.getByName(name) as AgentRpc;
  // Stub calls need `RuntimeContext`, which every Worker handler and every task execution has.
  // Erasing it keeps it out of user-visible types.
  return {
    enqueue: (taskId, task, input) =>
      stub.enqueue(taskId, task, input, { causation }) as Effect.Effect<Inbox.TaskRecord>,
    get: (taskId) => stub.get(taskId) as Effect.Effect<Inbox.TaskRecord | null>,
  };
};

/**
 * Binds an agent into a Worker (construction phase) and returns a typed lookup:
 * `const echo = yield* Agent.lookup(Echo)`, then `echo.getByName("a").send(...)`.
 */
export const lookup = <Ag extends AgentClass>(agent: Ag) =>
  Effect.map(
    agent as unknown as Effect.Effect<Namespace, never, Cloudflare.Worker>,
    (namespace): AgentLookup<Ag> => ({
      getByName: (name) => makeClient(agent, name, bindingTransport(namespace, name, null)),
    }),
  );

/** @internal Like `lookup`, but returns the raw transport per name (the API Worker uses it). */
export const lookupTransport = <Ag extends AgentClass>(agent: Ag) =>
  Effect.map(
    agent as unknown as Effect.Effect<Namespace, never, Cloudflare.Worker>,
    (namespace) => (name: string) => bindingTransport(namespace, name, null),
  );

const refKeys = new Map<string, Context.Key<any, any>>();
const refKey = (type: string): Context.Key<any, any> => {
  let key = refKeys.get(type);
  if (key === undefined) {
    key = Context.Service<any, any>(`alchemy-agent/AgentRef/${type}`);
    refKeys.set(type, key);
  }
  return key;
};

/** Inside a handler: `const echo = yield* Agent.use(Echo)`. Requires `uses: [Echo]`. */
export const use = <Ag extends AgentClass>(
  agent: Ag,
): Effect.Effect<AgentLookup<Ag>, never, AgentRef<AgentType<Ag>>> =>
  Effect.service(refKey(agent.definition.name)) as never;

type RefsOf<Uses extends ReadonlyArray<AgentClass>> = Uses[number] extends infer U
  ? U extends HasDefinition
    ? AgentRef<U["definition"]["name"]>
    : never
  : never;

// ---------------------------------------------------------------------------
// Testing hooks
// ---------------------------------------------------------------------------

const CurrentExecution = Context.Reference<{ readonly abort: (reason: string) => void } | undefined>(
  "alchemy-agent/CurrentExecution",
  { defaultValue: () => undefined },
);

export const Testing = {
  /**
   * Ends the current execution abruptly, as an eviction or a CPU-limit reset would: the
   * Durable Object is reset and the task is left `running`. For tests of redelivery only.
   */
  crashExecution: Effect.gen(function* () {
    const execution = yield* CurrentExecution;
    if (execution === undefined) return yield* Effect.die(new Error("not inside a task"));
    execution.abort("Testing.crashExecution");
    return yield* Effect.never;
  }),
};

// ---------------------------------------------------------------------------
// Agent.make: the Durable Object
// ---------------------------------------------------------------------------

const DEFAULT_RETRY = { maxAttempts: 3, delay: "1 second", maxDelay: "5 minutes" } as const;
/** Tasks run per drain pass before it reschedules itself, so one pass stays short. */
const DRAIN_BATCH = 10;
const DRAIN_CALLBACK = "alchemy-agent/drain";

const failureOf = (error: unknown): Inbox.StoredFailure =>
  typeof error === "object" && error !== null && "_tag" in error
    ? {
        _tag: String((error as { _tag: unknown })._tag),
        message: String((error as { message?: unknown }).message ?? ""),
      }
    : { _tag: "Defect", message: error instanceof Error ? error.message : String(error) };

/**
 * `class Echo extends Agent.make<Echo>()("Echo", { tasks }) {}`. The double call mirrors
 * `Cloudflare.DurableObject<Self>()`: `Self` is explicit, everything else inferred.
 */
export const make =
  <Self>() =>
  <
    const Name extends string,
    Tasks extends AnyTasks,
    const Uses extends ReadonlyArray<AgentClass> = readonly [],
  >(
    name: Name,
    definition: {
      /** The other agents this one sends to. Handlers reach them with `Agent.use`. */
      readonly uses?: Uses;
      /** Default retry policy for every task; each task can override it. */
      readonly retry?: RetryPolicy;
      readonly tasks: Tasks & CheckTasks<Tasks, AgentContext | RefsOf<Uses>>;
    },
  ) => {
    const tasks = definition.tasks as AnyTasks;
    const uses = (definition.uses ?? []) as ReadonlyArray<AgentClass>;
    const policyOf = (taskName: string) => {
      const p = { ...DEFAULT_RETRY, ...definition.retry, ...tasks[taskName]?.retry };
      return {
        maxAttempts: p.maxAttempts,
        delayMs: Duration.toMillis(Duration.fromInputUnsafe(p.delay)),
        maxDelayMs: Duration.toMillis(Duration.fromInputUnsafe(p.maxDelay)),
      };
    };
    const backoff = (taskName: string, attempts: number) => {
      const p = policyOf(taskName);
      return Math.min(p.maxDelayMs, p.delayMs * 2 ** Math.max(0, attempts - 1));
    };

    const base = Cloudflare.DurableObject<Self>()(
      name,
      Effect.gen(function* () {
        const state = yield* Cloudflare.DurableObjectState;
        // Construction phase: bind every agent this one uses (Alchemy only allows it here).
        const namespaces: Array<readonly [string, Namespace]> = [];
        for (const other of uses) {
          namespaces.push([other.definition.name, (yield* other) as Namespace]);
        }

        return Effect.gen(function* () {
          const sql = state.storage.sql as unknown as Inbox.Sql;
          const transaction = <A>(effect: Effect.Effect<A, never, RuntimeContext>) =>
            state.storage.transaction(effect) as Effect.Effect<A, never, RuntimeContext>;
          yield* Inbox.migrate(sql, transaction);
          const agentName = state.id.name ?? state.id.toString();
          const now = () => Date.now();

          // Runs one task's handler and reports what happened. Never fails.
          const execute = (row: Inbox.InboxRow, attempt: number) => {
            const def = tasks[row.task];
            if (def === undefined) {
              return Effect.succeed({
                _tag: "Failed" as const,
                permanent: true,
                error: { _tag: "UnknownTaskError", message: `No task "${row.task}"` },
              });
            }
            const refs = namespaces.map(
              ([type, namespace]) =>
                [
                  refKey(type),
                  {
                    getByName: (n: string) =>
                      makeClient(
                        uses.find((u) => u.definition.name === type)!,
                        n,
                        bindingTransport(namespace, n, `${name}/${agentName}/${row.task_id}`),
                      ),
                  },
                ] as const,
            );
            const services = <A, E>(effect: Effect.Effect<A, E, any>) =>
              refs
                .reduce(
                  (acc, [key, value]) => Effect.provideService(acc, key, value),
                  effect,
                )
                .pipe(
                  Effect.provideService(AgentContext, {
                    agent: { type: name, name: agentName },
                    taskId: row.task_id,
                    attempt,
                  }),
                  Effect.provideService(CurrentExecution, {
                    abort: (reason) => state.raw.abort(reason),
                  }),
                  Effect.annotateLogs({
                    agent: `${name}/${agentName}`,
                    taskId: row.task_id,
                    attempt,
                  }),
                ) as Effect.Effect<A, E>;

            return Effect.gen(function* () {
              const input = yield* Schema.decodeUnknownEffect(def.input)(JSON.parse(row.input)).pipe(
                Effect.mapError((issue) => new PermanentInput({ message: issue.message })),
              );
              const value = yield* services(def.handler(input));
              return def.output === undefined
                ? (value ?? null)
                : yield* Schema.encodeUnknownEffect(def.output as TaskSchema)(value).pipe(
                    Effect.orDie,
                  );
            }).pipe(
              Effect.map((result) => ({ _tag: "Completed" as const, result })),
              Effect.catchCause((cause) => {
                const [reason] = cause.reasons;
                if (reason?._tag === "Fail") {
                  const error = reason.error;
                  if (error instanceof PermanentInput) {
                    return Effect.succeed({
                      _tag: "Failed" as const,
                      permanent: true,
                      error: { _tag: "TaskInputError", message: error.message },
                    });
                  }
                  const failure = failureOf(error);
                  return Effect.succeed({
                    _tag: "Failed" as const,
                    permanent: failure._tag === "PermanentError",
                    error: failure,
                  });
                }
                return Effect.succeed({
                  _tag: "Failed" as const,
                  permanent: false,
                  error: {
                    _tag: "Defect",
                    message:
                      reason?._tag === "Die"
                        ? failureOf(reason.defect).message
                        : "the handler was interrupted",
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
            // The drain is single-flight per Durable Object, so a `running` row at the start of
            // a pass belongs to an execution that ended without settling it: a lost attempt.
            for (const row of yield* Inbox.running(sql)) {
              yield* transaction(
                afterFailure(
                  row,
                  row.attempts,
                  { _tag: "ExecutionLost", message: "the execution running this task ended" },
                  false,
                ),
              );
            }
            let processed = 0;
            while (processed < DRAIN_BATCH) {
              const row = yield* Inbox.nextDue(sql, now());
              if (row === undefined) break;
              const attempt = row.attempts + 1;
              // Persist `running` before the handler runs, so a crash counts as an attempt.
              yield* transaction(Inbox.markRunning(sql, row.seq, now()));
              const outcome = yield* execute(row, attempt);
              yield* transaction(
                outcome._tag === "Completed"
                  ? Inbox.complete(sql, row.seq, outcome.result, now())
                  : afterFailure(row, attempt, outcome.error, outcome.permanent),
              );
              processed++;
            }
            const next = yield* Inbox.nextRunAt(sql);
            if (next !== undefined) {
              // `progress` must strictly increase, or Alchemy counts the reschedule as a retry.
              const progress = yield* Inbox.advanceCursor(sql);
              yield* scheduleDrain(next, progress);
            }
          });

          drain = yield* Alchemy.makeCallback(DRAIN_CALLBACK, (_: { readonly v: 1 }) => drainPass, {
            retry: { delay: "1 second" },
          });

          const rpc: AgentRpc = {
            enqueue: (taskId, taskName, input, options) =>
              transaction(
                Effect.gen(function* () {
                  const def = tasks[taskName];
                  // Unknown tasks and invalid input are recorded as failed, not thrown.
                  const failure: Inbox.StoredFailure | undefined =
                    def === undefined
                      ? { _tag: "UnknownTaskError", message: `No task "${taskName}"` }
                      : yield* Schema.decodeUnknownEffect(def.input)(input).pipe(
                          Effect.as(undefined),
                          Effect.catch((issue) =>
                            Effect.succeed({ _tag: "TaskInputError", message: issue.message }),
                          ),
                        );
                  const { row, isNew } = yield* Inbox.insert(sql, {
                    taskId,
                    task: taskName,
                    input,
                    maxAttempts: policyOf(taskName).maxAttempts,
                    causation: options.causation,
                    now: now(),
                    ...(failure ? { failure } : {}),
                  });
                  // Scheduled in the same transaction as the write (D4).
                  if (isNew && !failure) yield* scheduleDrain(now());
                  return Inbox.toRecord(row);
                }),
              ),
            get: (taskId) =>
              Inbox.get(sql, taskId).pipe(
                Effect.map((row) => (row === undefined ? null : Inbox.toRecord(row))),
              ),
          };
          return rpc;
        });
      }),
    );

    return Object.assign(base, {
      definition: {
        name,
        tasks: definition.tasks as Tasks,
        retry: definition.retry,
      } as AgentDefinition<Name, Tasks>,
    });
  };

class PermanentInput extends Data.TaggedError("PermanentInput")<{ readonly message: string }> {}
