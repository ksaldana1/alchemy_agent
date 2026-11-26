// Agents: contracts built from Effect Rpcs, implementations as Layers, clients by name.
//
//   export const Greet = Rpc.make("Greet", { payload: { name: Schema.String }, success: Schema.String })
//   export const Greeter = Agent.make("Greeter", [Greet])                       // the contract
//   export const GreeterLive = Greeter.toLayer(Greeter.of({ Greet: ({ payload }) => … }))
//   yield* (yield* Greeter.client)("a").Greet({ name: "Ada" })                  // waits for the answer
//
// Modeled on effect/cluster's `Entity` (see docs/NORTH-STAR.md). One agent type is one Durable
// Object class (D2); its inbox is a SQLite table in each instance (D3), drained by a durable
// callback scheduled with the write that needs it (D4).
import * as Cloudflare from "@alchemy.run/cloudflare";
import type * as LanguageModel from "effect/ai/LanguageModel";
import type * as Tool from "effect/ai/Tool";
import type * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import type * as Duration from "effect/Duration";
import type * as Layer from "effect/Layer";
import type * as Schema from "effect/Schema";
import type { Rpc } from "effect/rpc";
import { makeClient, type AgentClient, type AgentContract, type AgentRpc } from "./internal/client.ts";
import { AgentError, Agents, Generating, Unreachable, type Address, type AgentContext, type ExternalTask, type GenerationError, type RetryPolicy } from "./internal/core.ts";
import { agentObject, type Handler } from "./internal/runtime.ts";
import type { Limits } from "./internal/session.ts";

export {
  Address,
  AgentContext,
  AgentError,
  AgentErrorReason,
  Agents,
  Canceled,
  ExecutionLost,
  GenerationError,
  GenerationFailed,
  InputConflict,
  InvalidPayload,
  ModelError,
  Retry,
  RetriesExhausted,
  StepLimit,
  TokenBudget,
  ToolCall,
  UnknownTask,
  Unreachable,
  WaitingInHandler,
  type RetryPolicy,
  type Transport,
} from "./internal/core.ts";
export type { AgentClient, AgentRpc, CallOptions, TranscriptEntry } from "./internal/client.ts";
export type { StoredFailure, TaskRecord, TaskStatus } from "./internal/inbox.ts";
export type { Limits, SessionEvent } from "./internal/session.ts";

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/** What a handler receives: the decoded payload, plus where and how it's running. */
export interface Request<R extends Rpc.Any, Type extends string = string> {
  readonly payload: Rpc.Payload<R>;
  readonly taskId: string;
  /** 1 on the first delivery, 2 on the first redelivery, and so on. */
  readonly attempt: number;
  readonly agent: Address<Type>;
}

/**
 * One handler per task. A handler fails with the task's declared `error` to settle it right
 * away (the caller gets that error); any other failure or defect is retried.
 */
export type HandlersFrom<Type extends string, Rpcs extends Rpc.Any> = {
  readonly [R in Rpcs as Rpc.Tag<R>]: (request: Request<R, Type>) => Effect.Effect<Rpc.Success<R>, unknown, any>;
};

type HandlerServices<H> = {
  [K in keyof H]: H[K] extends (...args: any) => Effect.Effect<any, any, infer R> ? R : never;
}[keyof H];

/** Services the framework provides to handlers itself. */
type Provided = AgentContext | Generating | ExternalTask;

/** Hosting an agent: what `toLayer` produces. A Worker provides it to host the agent. */
export interface AgentHost<Type extends string> {
  readonly _AgentHost: Type;
}

export interface ToLayerOptions<Tools extends Record<string, Tool.Any>> {
  readonly retry?: RetryPolicy;
  /** Tools for this agent's `Agent.generate` turns. Their handlers are a Layer (`Toolkit.toLayer`). */
  readonly toolkit?: Toolkit.Toolkit<Tools>;
  readonly limits?: Limits;
  /**
   * How long a settled task is kept, with its turns and calls. Default: forever. Pruning happens
   * while the agent has work, oldest first. A pruned task id is free again: sending it once more
   * records a new task, so keep this longer than any caller might retry a send.
   */
  readonly retention?: Duration.Input;
}

// ---------------------------------------------------------------------------
// Agent.make
// ---------------------------------------------------------------------------

export interface Agent<Type extends string, Rpcs extends Rpc.Any> {
  readonly type: Type;
  readonly protocol: ReadonlyArray<Rpcs>;
  /** Clients by agent name: `(yield* Greeter.client)("a").Greet(payload)`. */
  readonly client: Effect.Effect<(name: string) => AgentClient<Rpcs>>;
  /** This agent type's address for `name`, as data. */
  address(name: string): Address<Type>;
  of<const H extends HandlersFrom<Type, Rpcs>>(handlers: H): H;
  /**
   * The implementation, as a Layer: give it the handlers, or a construction Effect that builds
   * them. Whatever the handlers need beyond `AgentContext` (a model, tool handlers, your own
   * services) becomes a requirement of the Layer, provided with `Layer.provide`, as in Effect.
   */
  toLayer<const H extends HandlersFrom<Type, Rpcs>, RX = never, Tools extends Record<string, Tool.Any> = {}>(
    build: H | Effect.Effect<H, never, RX>,
    options?: ToLayerOptions<Tools>,
  ): Layer.Layer<
    AgentHost<Type>,
    never,
    Cloudflare.Worker | Exclude<RX | HandlerServices<H>, Provided> | ([keyof Tools] extends [never] ? never : Tool.HandlersFor<Tools>)
  >;
}

/**
 * An agent contract, from Effect `Rpc`s, like `Entity.make`. It holds no runtime code: workflows,
 * scripts and other agents import it to call the agent.
 */
export const make = <const Type extends string, const Rpcs extends ReadonlyArray<Rpc.Any>>(
  type: Type,
  protocol: Rpcs,
): Agent<Type, Rpcs[number]> => {
  const contract: AgentContract = {
    type,
    rpcs: new Map(protocol.map((rpc) => [rpc._tag, rpc as unknown as Rpc.AnyWithProps])),
  };
  return {
    type,
    protocol,
    client: Effect.map(Effect.service(Agents), (agents) => (name: string) => {
      const address = { type, name };
      return makeClient<Rpcs[number]>(contract, address, agents.transport(address));
    }),
    address: (name) => ({ type, name }),
    of: (handlers) => handlers,
    toLayer: (build, options) => {
      // Alchemy's declaration form: `.make(impl)` hosts the Durable Object class and binds it in
      // the Worker that provides the Layer; the implementation's requirements become the Layer's.
      const Declaration = Cloudflare.DurableObject<AgentHost<Type>, AgentRpc>()(type);
      return Declaration.make(
        agentObject({
          contract,
          build: build as unknown as Record<string, Handler> | Effect.Effect<Record<string, Handler>, never, any>,
          retry: options?.retry,
          toolkit: options?.toolkit as Toolkit.Any | undefined,
          limits: options?.limits,
          retention: options?.retention,
        }),
      ) as never;
    },
  };
};

// ---------------------------------------------------------------------------
// Agent.generate
// ---------------------------------------------------------------------------

export interface GenerateOptions<S extends Schema.Codec<any, any, never, never>> {
  readonly prompt: string;
  readonly system?: string | undefined;
  /** Continue a conversation. Default: a new session for this call. */
  readonly session?: string | undefined;
  /**
   * The answer's schema. Default `Schema.String` (the model's text). A schema whose encoded form
   * is a string is answered in text and decoded; anything else is answered through a `respond`
   * tool built from the schema. An answer that doesn't decode goes back to the model.
   */
  readonly schema?: S;
}

/**
 * One durable LLM turn: every model call and tool call is a checkpointed step. Only in handlers
 * of an agent whose Layer has a model (`AgentModel.*`). The task waits (holding no execution)
 * while the turn runs, then the handler runs again from the top and this call returns the stored
 * answer, so code before it can run more than once.
 */
export const generate = <S extends Schema.Codec<any, any, never, never> = typeof Schema.String>(
  options: GenerateOptions<S>,
): Effect.Effect<S["Type"], GenerationError, LanguageModel.LanguageModel> =>
  Effect.flatMap(Effect.service(Generating), (g) =>
    g.run({ prompt: options.prompt, system: options.system, session: options.session, schema: options.schema }),
  ) as never;

// ---------------------------------------------------------------------------
// Agent.cancel
// ---------------------------------------------------------------------------

/**
 * Cancels a task that hasn't settled. It settles as failed with `AgentError(Canceled)`, which is
 * what anyone waiting for it gets. Its LLM turns end, and calls it made that weren't delivered yet
 * are dropped (tasks it already sent to other agents keep running). A handler running at the time
 * is interrupted, and nothing it does afterwards is recorded.
 *
 * Succeeds with `true` if this call canceled the task, `false` if it had already settled. An
 * unknown task fails with `AgentError(Unreachable)`. A keyed task's id is `<Tag>:<primaryKey>`.
 */
export const cancel = (
  agent: Address,
  taskId: string,
  options?: { readonly reason?: string },
): Effect.Effect<boolean, AgentError> =>
  Effect.flatMap(Effect.service(Agents), (agents) =>
    Effect.flatMap(agents.transport(agent).cancel(taskId, options?.reason ?? ""), ({ canceled, record }) =>
      record === null
        ? Effect.fail(new AgentError({ agent, reason: new Unreachable({ message: `no task ${taskId}` }) }))
        : Effect.succeed(canceled),
    ),
  );
