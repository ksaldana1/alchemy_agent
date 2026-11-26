// API reset prototype: TYPES ONLY. Every value is `declare`d; nothing here runs.
// Mirrors effect/cluster's Entity over effect/rpc's Rpc (see docs/NORTH-STAR.md).
import type * as LanguageModel from "effect/ai/LanguageModel";
import type * as Toolkit from "effect/ai/Toolkit";
import type * as Tool from "effect/ai/Tool";
import type * as Context from "effect/Context";
import type * as Duration from "effect/Duration";
import type * as Effect from "effect/Effect";
import type * as Layer from "effect/Layer";
import type * as Schema from "effect/Schema";
import type { Rpc } from "effect/rpc";

// ---------------------------------------------------------------------------
// Errors (Schema.TaggedError, with a tagged reason)
// ---------------------------------------------------------------------------

export interface Unreachable { readonly _tag: "Unreachable"; readonly message: string }
export interface ExecutionLost { readonly _tag: "ExecutionLost"; readonly attempts: number }
export interface RetriesExhausted { readonly _tag: "RetriesExhausted"; readonly attempts: number; readonly lastError: string }
export interface InvalidPayload { readonly _tag: "InvalidPayload"; readonly message: string }
export interface UnknownTask { readonly _tag: "UnknownTask"; readonly tag: string }

/** A task couldn't be delivered or didn't settle with an answer. Handle with `Effect.catchReason`. */
export interface AgentError {
  readonly _tag: "AgentError";
  readonly agent: Address;
  readonly reason: Unreachable | ExecutionLost | RetriesExhausted | InvalidPayload | UnknownTask;
}

export interface StepLimit { readonly _tag: "StepLimit"; readonly steps: number }
export interface TokenBudget { readonly _tag: "TokenBudget"; readonly tokens: number }
export interface ModelError { readonly _tag: "ModelError"; readonly message: string }

/** A durable LLM turn ended without an answer. */
export interface GenerationError {
  readonly _tag: "GenerationError";
  readonly reason: StepLimit | TokenBudget | ModelError;
}

// ---------------------------------------------------------------------------
// Addresses, the handler envelope, and services
// ---------------------------------------------------------------------------

/** An agent's address, passed around as data. */
export interface Address<Type extends string = string> {
  readonly type: Type;
  readonly name: string;
}
export declare const Address: Schema.Codec<Address, Address>;

/** What a handler receives: the decoded payload, plus where and how it's running. */
export interface Request<R extends Rpc.Any, Type extends string = string> {
  readonly payload: Rpc.Payload<R>;
  readonly taskId: string;
  /** 1 on the first delivery, 2 on the first redelivery, and so on. */
  readonly attempt: number;
  readonly agent: Address<Type>;
}

/** The same, as a service, for code deeper in the call stack. */
export interface AgentContext {
  readonly _AgentContext: unique symbol;
}

/** Waiting for an answer. Workers and HTTP clients have it; handlers get it in M3. */
export interface Waiter {
  readonly _Waiter: unique symbol;
}

/** How clients reach agents: bindings inside Cloudflare, HTTP outside (`Api.connect`). */
export interface Agents {
  readonly _Agents: unique symbol;
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

export interface CallOptions<Discard extends boolean = false> {
  /** Send without waiting for the answer. */
  readonly discard?: Discard;
}

/** One agent, by name. Each task is a method, like an Entity client. */
export type AgentClient<Rpcs extends Rpc.Any> = {
  readonly [R in Rpcs as Rpc.Tag<R>]: {
    (...args: [...PayloadArg<R>, options?: CallOptions<false>]): Effect.Effect<
      Rpc.Success<R>,
      Rpc.Error<R> | AgentError,
      Waiter
    >;
    (...args: [...PayloadArg<R>, options: CallOptions<true>]): Effect.Effect<void, AgentError>;
  };
};
/** A task with no payload takes none. */
type PayloadArg<R> = [Rpc.PayloadConstructor<R>] extends [void] ? [payload?: void] : [payload: Rpc.PayloadConstructor<R>];

// ---------------------------------------------------------------------------
// Agent.make
// ---------------------------------------------------------------------------

export interface RetryPolicy {
  readonly maxAttempts?: number;
  readonly delay?: Duration.Input;
  readonly maxDelay?: Duration.Input;
}

export type HandlersFrom<Type extends string, Rpcs extends Rpc.Any> = {
  readonly [R in Rpcs as Rpc.Tag<R>]: (
    request: Request<R, Type>,
  ) => Effect.Effect<Rpc.Success<R>, Rpc.Error<R> | (unknown & {}), any>;
};

type HandlerServices<H> = {
  [K in keyof H]: H[K] extends (...args: any) => Effect.Effect<any, any, infer R> ? R : never;
}[keyof H];

/** Hosting an agent: what `toLayer` produces, and what a Worker or platform provides. */
export interface AgentHost<Type extends string> {
  readonly _AgentHost: Type;
}

export interface Agent<Type extends string, Rpcs extends Rpc.Any> {
  readonly type: Type;
  readonly protocol: ReadonlyArray<Rpcs>;
  /** Clients by name: `(yield* Greeter.client)("a").Greet(payload)`. */
  readonly client: Effect.Effect<(name: string) => AgentClient<Rpcs>, never, Agents>;
  address(name: string): Address<Type>;
  of<const H extends HandlersFrom<Type, Rpcs>>(handlers: H): H;
  /**
   * The implementation, as a Layer. Handlers may use `AgentContext` and send without waiting;
   * anything else they need (a model, tool handlers, other services) becomes a requirement of
   * the Layer, provided with `Layer.provide`, as in Effect.
   */
  toLayer<const H extends HandlersFrom<Type, Rpcs>, RX = never>(
    build: H | Effect.Effect<H, never, RX>,
    options?: { readonly retry?: RetryPolicy },
  ): Layer.Layer<
    AgentHost<Type>,
    never,
    Exclude<RX | HandlerServices<H>, AgentContext | Agents | Waiter> | ([Extract<HandlerServices<H>, Waiter>] extends [never] ? never : WaitingInHandler)
  >;
}

/** A handler that waits for another agent's answer: not possible until M3. */
export interface WaitingInHandler {
  readonly "Handlers can't wait for another agent yet (M3): call with { discard: true }": unique symbol;
}

export declare const make: <const Type extends string, const Rpcs extends ReadonlyArray<Rpc.Any>>(
  type: Type,
  protocol: Rpcs,
) => Agent<Type, Rpcs[number]>;

// ---------------------------------------------------------------------------
// Durable LLM turns, shaped like Effect AI's LanguageModel calls
// ---------------------------------------------------------------------------

export interface GenerateOptions<Tools extends Record<string, Tool.Any>> {
  readonly prompt: string;
  readonly system?: string;
  readonly toolkit?: Toolkit.Toolkit<Tools>;
  /** Continue a conversation. Default: a new session for this call. */
  readonly session?: string;
}

/**
 * One durable LLM turn. Without a `schema` it returns the model's text (the schema defaults to
 * `Schema.String`, as an Rpc's `success` defaults to `Schema.Void`). With one, the answer is
 * decoded with it:
 * - a schema whose encoded form is a string (`Schema.String`, `Schema.NonEmptyString`, a branded
 *   string, `Schema.NumberFromString`…): the model answers in text, decoded by the schema
 * - anything else (structs, literals, arrays…): the model answers through a `respond` tool built
 *   from the schema's JSON Schema
 * Either way, an answer that doesn't decode goes back to the model to correct, within the turn.
 */
export declare const generate: <
  S extends Schema.Codec<any, any, never, never> = typeof Schema.String,
  Tools extends Record<string, Tool.Any> = {},
>(
  options: GenerateOptions<Tools> & { readonly schema?: S },
) => Effect.Effect<S["Type"], GenerationError, LanguageModel.LanguageModel | Tool.HandlersFor<Tools>>;

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

export declare const AgentModel: {
  workersAI(model: string): Layer.Layer<LanguageModel.LanguageModel>;
  scripted(script: ReadonlyArray<unknown>): Layer.Layer<LanguageModel.LanguageModel>;
};

export declare const AgentContextTag: Context.Key<AgentContext, Request<Rpc.Any>>;
