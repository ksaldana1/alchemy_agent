// Agent references: `Greeter.named("a")`. A reference is plain data plus methods; how it reaches
// the agent comes from the context (`Reach<Type>`), so the same reference works in a Worker, in a
// handler, and over HTTP.
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import {
  AgentUnreachable,
  reachKey,
  TaskFailed,
  TaskNotFound,
  type AgentClass,
  type AgentType,
  type Reach,
  type TaskError,
  type TaskFailure,
  type TaskInput,
  type TaskNames,
  type TaskOutput,
  type TaskSchema,
  type Transport,
  type Waiter,
} from "./core.ts";
import type { TaskRecord } from "./inbox.ts";

export interface CallOptions {
  /** The task id, which doubles as the idempotency key. Default: a random UUID. */
  readonly taskId?: string;
}

/** An agent's address, passed around as data (e.g. "share this file with that agent"). */
export interface Address<Type extends string = string> {
  readonly type: Type;
  readonly name: string;
}

export interface TaskHandle<Ag, K extends TaskNames<Ag>> {
  readonly taskId: string;
  readonly task: K;
  /** The stored record, as it is now. */
  readonly status: Effect.Effect<TaskRecord, TaskNotFound | AgentUnreachable, Reach<AgentType<Ag>>>;
  /** Waits until the task settles: its output, or `TaskFailed`. */
  readonly result: Effect.Effect<
    TaskOutput<Ag, K>,
    TaskFailed<TaskError<Ag, K>> | TaskNotFound | AgentUnreachable,
    Reach<AgentType<Ag>> | Waiter
  >;
}

/** Names a task can't have, because the reference uses them. */
export type ReservedTaskName = "send" | "handle" | "address";

/** A task method's parameters: the input may be omitted when the task takes none. */
type Args<Ag, K extends TaskNames<Ag>> = {} extends TaskInput<Ag, K>
  ? [input?: TaskInput<Ag, K>, options?: CallOptions]
  : [input: TaskInput<Ag, K>, options?: CallOptions];

/**
 * A typed reference to one agent. Each task is a method that sends and waits:
 * `yield* Greeter.named("a").greet({ name })`. `send.<task>` returns a handle without waiting.
 */
export type AgentRef<Ag> = {
  readonly [K in TaskNames<Ag>]: (
    ...args: Args<Ag, K>
  ) => Effect.Effect<
    TaskOutput<Ag, K>,
    TaskFailed<TaskError<Ag, K>> | AgentUnreachable,
    Reach<AgentType<Ag>> | Waiter
  >;
} & {
  readonly send: {
    readonly [K in TaskNames<Ag>]: (
      ...args: Args<Ag, K>
    ) => Effect.Effect<TaskHandle<Ag, K>, AgentUnreachable, Reach<AgentType<Ag>>>;
  };
  handle<K extends TaskNames<Ag>>(task: K, taskId: string): TaskHandle<Ag, K>;
  readonly address: Address<AgentType<Ag>>;
};

const isSettled = (r: TaskRecord) => r.status === "completed" || r.status === "failed";

/** The outcome of a settled record: decoded output, or `TaskFailed`. */
const outcome = (agent: AgentClass, record: TaskRecord) => {
  if (record.status === "failed") {
    return Effect.fail(
      new TaskFailed({
        taskId: record.taskId,
        task: record.task,
        attempts: record.attempts,
        error: (record.error ?? {
          _tag: "Defect",
          message: "failed without an error",
        }) as TaskFailure<unknown>,
      }),
    );
  }
  const output = agent.definition.tasks[record.task]?.output as TaskSchema | undefined;
  return output === undefined
    ? Effect.succeed(record.result)
    : Schema.decodeUnknownEffect(output)(record.result).pipe(Effect.orDie);
};

/** Waits for a task to settle, by asking the transport to wait (it returns periodically). */
const awaitSettled = (transport: Transport, taskId: string) =>
  transport.wait(taskId).pipe(
    Effect.repeat({ until: (r) => r === null || isSettled(r) }),
    Effect.flatMap((r) => (r === null ? Effect.fail(new TaskNotFound({ taskId })) : Effect.succeed(r))),
  );

export const makeRef = <Ag extends AgentClass>(agent: Ag, name: string): AgentRef<Ag> => {
  const type = agent.definition.name;
  const transport = Effect.map(Effect.service(reachKey(type)), (reach) => reach.transport(name));

  const handle = (task: string, taskId: string) => ({
    taskId,
    task,
    status: Effect.flatMap(transport, (t) => t.get(taskId)).pipe(
      Effect.flatMap((r) => (r === null ? Effect.fail(new TaskNotFound({ taskId })) : Effect.succeed(r))),
    ),
    result: Effect.flatMap(transport, (t) => awaitSettled(t, taskId)).pipe(
      Effect.flatMap((r) => outcome(agent, r)),
    ),
  });

  const enqueue = (task: string, input: unknown, options: CallOptions | undefined) =>
    Effect.flatMap(transport, (t) =>
      Effect.suspend(() => t.enqueue(options?.taskId ?? crypto.randomUUID(), task, input ?? {})),
    );

  const calls: Record<string, unknown> = {};
  const sends: Record<string, unknown> = {};
  for (const task of Object.keys(agent.definition.tasks)) {
    calls[task] = (input?: unknown, options?: CallOptions) =>
      Effect.gen(function* () {
        const t = yield* transport;
        const recorded = yield* enqueue(task, input, options);
        // The record exists (we just wrote it), so TaskNotFound can't happen here.
        const settled = yield* awaitSettled(t, recorded.taskId).pipe(Effect.orDie);
        return yield* outcome(agent, settled);
      });
    sends[task] = (input?: unknown, options?: CallOptions) =>
      Effect.map(enqueue(task, input, options), (r) => handle(r.task, r.taskId));
  }
  return {
    ...calls,
    send: sends,
    handle,
    address: { type, name },
  } as unknown as AgentRef<Ag>;
};

// ---------------------------------------------------------------------------
// The transport over a Durable Object binding
// ---------------------------------------------------------------------------

/**
 * The agent Durable Object's RPC surface. Framework-internal. A `type`, not an `interface`:
 * Alchemy's `MainRpc` constraint is an index signature, which interfaces don't satisfy.
 */
export type AgentRpc = {
  enqueue: (
    taskId: string,
    task: string,
    input: unknown,
    options: { readonly causation: string | null },
  ) => Effect.Effect<TaskRecord, never, any>;
  get: (taskId: string) => Effect.Effect<TaskRecord | null, never, any>;
  wait: (taskId: string, timeoutMs: number) => Effect.Effect<TaskRecord | null, never, any>;
};

/** How long one `wait` RPC holds before returning the current record. */
export const WAIT_CHUNK_MS = 20_000;

/**
 * A failed RPC (e.g. the Durable Object reset mid-call) becomes `AgentUnreachable`, after a few
 * retries. Every operation is idempotent, so retrying is safe.
 */
const reliably = <A>(effect: Effect.Effect<A, never, any>): Effect.Effect<A, AgentUnreachable> =>
  (effect as unknown as Effect.Effect<A, unknown>).pipe(
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.failCause(cause as Cause.Cause<never>)
        : Effect.fail(new AgentUnreachable({ message: String(Cause.squash(cause)) })),
    ),
    Effect.retry({ schedule: Schedule.exponential("200 millis"), times: 6 }),
  );

export interface Namespace {
  readonly getByName: (name: string) => unknown;
}

export const bindingTransport = (
  namespace: Namespace,
  name: string,
  causation: string | null,
): Transport => {
  // Stub calls need Alchemy's `RuntimeContext`, which every Worker handler and every task
  // execution has; it stays out of user-visible types.
  const stub = () => namespace.getByName(name) as AgentRpc;
  return {
    enqueue: (taskId, task, input) => reliably(Effect.suspend(() => stub().enqueue(taskId, task, input, { causation }))),
    get: (taskId) => reliably(Effect.suspend(() => stub().get(taskId))),
    // If `wait` fails (for instance, right after a deploy an instance can still run code from
    // before `wait` existed), fall back to reading the record after a short pause: waiting is
    // never worse than polling. See docs/spike/0.6-verified-facts.md (version skew).
    wait: (taskId) =>
      (Effect.suspend(() => stub().wait(taskId, WAIT_CHUNK_MS)) as unknown as Effect.Effect<TaskRecord | null, unknown>).pipe(
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.failCause(cause as Cause.Cause<never>)
            : Effect.sleep("1 second").pipe(Effect.andThen(reliably(Effect.suspend(() => stub().get(taskId))))),
        ),
      ),
  };
};
