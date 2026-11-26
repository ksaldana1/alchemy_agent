// Shared building blocks: errors, the address, services agent code sees, and the transport every
// way of reaching an agent implements.
import * as Context from "effect/Context";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { TaskRecord } from "./inbox.ts";

// ---------------------------------------------------------------------------
// Addresses
// ---------------------------------------------------------------------------

/** An agent's address: its type (the `Agent.make` name) and its name. Plain data. */
export const Address = Schema.Struct({ type: Schema.String, name: Schema.String });
export interface Address<Type extends string = string> {
  readonly type: Type;
  readonly name: string;
}

// ---------------------------------------------------------------------------
// Errors: one tagged error per concern, each with a tagged reason (see effect's
// ai-docs/src/01_effect/04_errors/20_reason-errors.ts)
// ---------------------------------------------------------------------------

/** The agent couldn't be reached (network, API, or a Durable Object reset mid-call). */
export class Unreachable extends Schema.TaggedError<Unreachable>()("Unreachable", {
  message: Schema.String,
}) {}
/** The execution running the task kept disappearing (eviction, CPU limit, crash). */
export class ExecutionLost extends Schema.TaggedError<ExecutionLost>()("ExecutionLost", {
  attempts: Schema.Number,
}) {}
/** The handler failed with an undeclared error on every attempt. */
export class RetriesExhausted extends Schema.TaggedError<RetriesExhausted>()("RetriesExhausted", {
  attempts: Schema.Number,
  lastError: Schema.String,
}) {}
/** The payload didn't decode against the task's schema. */
export class InvalidPayload extends Schema.TaggedError<InvalidPayload>()("InvalidPayload", {
  message: Schema.String,
}) {}
/** The agent has no task with this tag. */
export class UnknownTask extends Schema.TaggedError<UnknownTask>()("UnknownTask", {
  tag: Schema.String,
}) {}
/** A durable LLM turn the handler started ended without an answer. */
export class GenerationFailed extends Schema.TaggedError<GenerationFailed>()("GenerationFailed", {
  message: Schema.String,
}) {}
/** Waiting for another agent from inside a handler (not possible until M3). */
export class WaitingInHandler extends Schema.TaggedError<WaitingInHandler>()("WaitingInHandler", {
  message: Schema.String,
}) {}

export const AgentErrorReason = Schema.Union([
  Unreachable,
  ExecutionLost,
  RetriesExhausted,
  InvalidPayload,
  UnknownTask,
  GenerationFailed,
  WaitingInHandler,
]);

/**
 * A task couldn't be delivered, or didn't settle with an answer. A handler's *declared* errors
 * reach the caller as themselves; this is everything else. Handle with `Effect.catchReason`.
 */
export class AgentError extends Schema.TaggedError<AgentError>()("AgentError", {
  agent: Address,
  reason: AgentErrorReason,
}) {}

export class StepLimit extends Schema.TaggedError<StepLimit>()("StepLimit", { steps: Schema.Number }) {}
export class TokenBudget extends Schema.TaggedError<TokenBudget>()("TokenBudget", { tokens: Schema.Number }) {}
export class ModelError extends Schema.TaggedError<ModelError>()("ModelError", { message: Schema.String }) {}

/** A durable LLM turn ended without an answer. */
export class GenerationError extends Schema.TaggedError<GenerationError>()("GenerationError", {
  reason: Schema.Union([StepLimit, TokenBudget, ModelError]),
}) {}

// ---------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------

/** The running task: which agent, which task, which attempt. Also passed to handlers. */
export class AgentContext extends Context.Service<
  AgentContext,
  {
    readonly agent: Address;
    readonly taskId: string;
    /** 1 on the first delivery, 2 on the first redelivery, and so on. */
    readonly attempt: number;
  }
>()("alchemy-agent/AgentContext") {}

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

/** How a client reaches one agent. Every operation is idempotent, so retrying is safe. */
export interface Transport {
  readonly enqueue: (taskId: string, tag: string, payload: unknown) => Effect.Effect<TaskRecord, AgentError>;
  readonly get: (taskId: string) => Effect.Effect<TaskRecord | null, AgentError>;
  /** Returns once the task settles, or after a while with its current record (callers loop). */
  readonly wait: (taskId: string) => Effect.Effect<TaskRecord | null, AgentError>;
}

/**
 * How clients reach agents. Inside Cloudflare the default resolves Durable Object bindings from
 * the Worker's environment at call time; outside, `Api.connect` replaces it with HTTP.
 */
export class Agents extends Context.Reference<{
  readonly transport: (agent: Address) => Transport;
}>("alchemy-agent/Agents", {
  defaultValue: () => ({ transport: (agent) => bindingTransportRef.current!(agent) }),
}) {}
/** Set by `internal/client.ts` (avoids an import cycle). */
export const bindingTransportRef: { current: ((agent: Address) => Transport) | undefined } = { current: undefined };

/** The current task execution (tests crash and count with it; the wait guard reads it). */
export const CurrentExecution = Context.Reference<
  | {
      readonly abort: (reason: string) => void;
      /** Increments a durable counter (tests count executions across crashes). */
      readonly count: (name: string) => Effect.Effect<number>;
    }
  | undefined
>("alchemy-agent/CurrentExecution", { defaultValue: () => undefined });

// ---------------------------------------------------------------------------
// Durable generation (internal service; `Agent.generate` reads it)
// ---------------------------------------------------------------------------

export interface GenerateRequest {
  readonly prompt: string;
  readonly system: string | undefined;
  readonly session: string | undefined;
  readonly schema: Schema.Codec<any, any, never, never> | undefined;
}
export class Generating extends Context.Service<
  Generating,
  { readonly run: (request: GenerateRequest) => Effect.Effect<unknown, GenerationError> }
>()("alchemy-agent/Generating") {}

/**
 * Raised (as a defect, so handler error handling doesn't swallow it) when a handler reaches an
 * unfinished turn: the task suspends and resumes when the turn settles.
 */
export class Suspend {
  readonly _tag = "alchemy-agent/Suspend";
  constructor(readonly turnId: string) {}
}

// ---------------------------------------------------------------------------
// Retry policy
// ---------------------------------------------------------------------------

export interface RetryPolicy {
  /** Deliveries before the task fails. Default 3. */
  readonly maxAttempts?: number;
  /** Delay before the first redelivery; doubles each time. Default 1 second. */
  readonly delay?: Duration.Input;
  /** Upper bound on the delay. Default 5 minutes. */
  readonly maxDelay?: Duration.Input;
}

/**
 * A task's own retry policy, as an Rpc annotation (like `ClusterSchema.Persisted`). It overrides
 * the agent's policy: `Rpc.make("Sync", { ... }).annotate(Agent.Retry, { delay: "30 seconds" })`.
 */
export const Retry = Context.Reference<RetryPolicy | undefined>("alchemy-agent/Retry", { defaultValue: () => undefined });

export const isEffect = Effect.isEffect;
