// Shared building blocks: services agent code sees, errors, task definitions, helper types, and
// the transport interface every way of reaching an agent implements.
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { TaskRecord } from "./inbox.ts";

// ---------------------------------------------------------------------------
// Services
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

/**
 * Reaching agent `Type`. Provided in a Worker by `Agent.connect`, inside a handler by the
 * framework (for the agents in `uses`), and outside Cloudflare by `Api.connect`.
 */
export interface Reach<Type extends string> {
  readonly _Reach: Type;
}

/**
 * Waiting for a task to settle. Workers and HTTP clients have it. Handlers don't until M3, where
 * waiting suspends the task instead of holding execution.
 */
export interface Waiter {
  readonly _Waiter: unique symbol;
}

/** How a reference reaches one agent. Every operation is idempotent, so retrying is safe. */
export interface Transport {
  readonly enqueue: (
    taskId: string,
    task: string,
    input: unknown,
  ) => Effect.Effect<TaskRecord, AgentUnreachable>;
  readonly get: (taskId: string) => Effect.Effect<TaskRecord | null, AgentUnreachable>;
  /** Returns once the task settles, or after a while with its current record (callers loop). */
  readonly wait: (taskId: string) => Effect.Effect<TaskRecord | null, AgentUnreachable>;
}

export interface ReachService {
  readonly transport: (name: string) => Transport;
}

const reachKeys = new Map<string, Context.Key<any, ReachService>>();
export const reachKey = (type: string): Context.Key<any, ReachService> => {
  let key = reachKeys.get(type);
  if (key === undefined) {
    key = Context.Service<any, ReachService>(`alchemy-agent/Reach/${type}`);
    reachKeys.set(type, key);
  }
  return key;
};

export const waiterKey = Context.Service<Waiter, true>("alchemy-agent/Waiter");

/** The current task execution; read by `alchemy-agent/testing`. */
export const CurrentExecution = Context.Reference<
  | {
      readonly abort: (reason: string) => void;
      /** Increments a durable counter (tests count executions across crashes). */
      readonly count: (name: string) => Effect.Effect<number>;
    }
  | undefined
>("alchemy-agent/CurrentExecution", { defaultValue: () => undefined });

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Fail with this to settle the task right away, without further attempts. */
export class PermanentError extends Schema.TaggedError<PermanentError>()("PermanentError", {
  message: Schema.String,
}) {}

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

/** The task settled as failed. */
export class TaskFailed<E = unknown> extends Data.TaggedError("TaskFailed")<{
  readonly taskId: string;
  readonly task: string;
  readonly attempts: number;
  readonly error: TaskFailure<E>;
}> {}

/** No task with this id was ever recorded for this agent. */
export class TaskNotFound extends Data.TaggedError("TaskNotFound")<{
  readonly taskId: string;
}> {}

/**
 * The agent couldn't be reached: a network or API error, or the Durable Object reset mid-call.
 * Retrying with the same task id is always safe.
 */
export class AgentUnreachable extends Data.TaggedError("AgentUnreachable")<{
  readonly message: string;
}> {}

// ---------------------------------------------------------------------------
// LLM turns
// ---------------------------------------------------------------------------

/** A prompt's turn ended without an answer. */
export class PromptFailed extends Data.TaggedError("PromptFailed")<{
  readonly reason: "StepLimit" | "TokenBudget" | "ModelError" | "InvalidOutput";
  readonly message: string;
}> {}

/** Running prompts. Only agents with a `model` have it. */
export interface Prompting {
  readonly _Prompting: unique symbol;
}

export interface PromptOptions {
  /** The session (conversation) to continue. Default: a new session for this prompt. */
  readonly session?: string;
  /** A schema for a structured answer; the model gives it through a `respond` tool. */
  readonly output?: TaskSchema;
}

export interface PromptService {
  readonly run: (message: string, options: PromptOptions) => Effect.Effect<unknown, PromptFailed>;
}
export const promptingKey = Context.Service<Prompting, PromptService>("alchemy-agent/Prompting");

/**
 * Raised (as a defect, so handlers' error handling doesn't swallow it) when a handler reaches a
 * prompt whose turn hasn't finished: the task suspends and resumes when the turn settles.
 */
export class Suspend {
  readonly _tag = "alchemy-agent/Suspend";
  constructor(readonly turnId: string) {}
}

/** The tool call being executed. `idempotencyKey` is stable across redeliveries of the call. */
export class ToolCall extends Context.Service<
  ToolCall,
  {
    readonly sessionId: string;
    readonly turnId: string;
    readonly toolCallId: string;
    readonly idempotencyKey: string;
  }
>()("alchemy-agent/ToolCall") {}

// ---------------------------------------------------------------------------
// Task definitions and helper types
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

export const TaskDefTypeId: unique symbol = Symbol.for("alchemy-agent/TaskDef") as never;

export interface TaskDef<In extends TaskSchema, Out extends TaskSchema | undefined, A, E, R> {
  readonly [TaskDefTypeId]: typeof TaskDefTypeId;
  readonly input: In;
  readonly output: Out;
  readonly retry: RetryPolicy | undefined;
  readonly handler: (input: In["Type"]) => Effect.Effect<A, E, R>;
}

// `any` for the input schema: `TaskDef` is invariant in it (it's also the handler's parameter).
export type AnyTaskDef = TaskDef<any, any, any, any, any>;
export type AnyTasks = Record<string, AnyTaskDef>;

export const isTaskDef = (u: unknown): u is AnyTaskDef =>
  typeof u === "object" && u !== null && TaskDefTypeId in u;

export interface AgentDefinition<Name extends string, Tasks extends AnyTasks> {
  readonly name: Name;
  readonly tasks: Tasks;
  readonly retry: RetryPolicy | undefined;
}

/** Any class made by `Agent.make`. */
export interface AgentClass {
  readonly definition: AgentDefinition<string, AnyTasks>;
}

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
export type ServicesOf<T> = T extends TaskDef<any, any, any, any, infer R> ? R : never;
export type RawOutputOf<T> = T extends TaskDef<any, infer Out, infer A, any, any>
  ? Out extends TaskSchema
    ? Json
    : A
  : never;
