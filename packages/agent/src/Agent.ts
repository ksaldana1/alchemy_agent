// Agent definitions and references.
//
//   export class Greeter extends Agent.make<Greeter>()("Greeter", {
//     greet: Agent.task({ name: Schema.String }, ({ name }) => Effect.succeed(`Hello, ${name}!`)),
//   }) {}
//
//   yield* Greeter.named("a").greet({ name: "Ada" })          // send and wait
//   yield* Greeter.named("a").send.greet({ name: "Ada" })     // send; returns a handle
//
// One agent definition is one Durable Object class (D2). Its inbox is a SQLite table inside each
// instance (D3), drained by a durable callback scheduled with the write that needs it (D4).
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Tool from "effect/ai/Tool";
import type * as Toolkit from "effect/ai/Toolkit";
import * as Schema from "effect/Schema";
import type { AgentModel } from "./AgentModel.ts";
import {
  promptingKey,
  type PromptFailed,
  type Prompting,
  type ToolCall,
  reachKey,
  TaskDefTypeId,
  waiterKey,
  isTaskDef,
  type AgentClass,
  type AgentContext,
  type AgentDefinition,
  type AgentType,
  type AnyTasks,
  type Json,
  type Reach,
  type RawOutputOf,
  type RetryPolicy,
  type ServicesOf,
  type TaskDef,
  type TaskSchema,
  type Waiter,
} from "./internal/core.ts";
import { bindingTransport, makeRef, type AgentRef, type Namespace, type ReservedTaskName } from "./internal/ref.ts";
import { agentObject } from "./internal/runtime.ts";
import type { Limits, ToolsDef } from "./internal/session.ts";

export {
  AgentContext,
  AgentUnreachable,
  PermanentError,
  PromptFailed,
  ToolCall,
  type Prompting,
  TaskFailed,
  TaskNotFound,
  type AgentClass,
  type AgentDefinition,
  type AgentType,
  type AnyTaskDef,
  type Json,
  type Reach,
  type RetryPolicy,
  type TaskDef,
  type TaskError,
  type TaskFailure,
  type TaskInput,
  type TaskInputDecoded,
  type TaskNames,
  type TaskOutput,
  type TaskSchema,
  type Waiter,
} from "./internal/core.ts";
export type { StoredFailure, TaskRecord, TaskStatus } from "./internal/inbox.ts";
/** `AgentRpc` is the Durable Object's internal RPC surface, exported only so class types are nameable. */
export type { Address, AgentRef, AgentRpc, CallOptions, ReservedTaskName, TaskHandle, TranscriptEntry } from "./internal/ref.ts";
export type { Limits, SessionEvent } from "./internal/session.ts";

// ---------------------------------------------------------------------------
// Agent.task
// ---------------------------------------------------------------------------

/** A task's input: struct fields (the common case) or a full schema. */
export type TaskInputSpec = Schema.Struct.Fields | TaskSchema;
type ToSchema<I> = I extends TaskSchema
  ? I
  : I extends Schema.Struct.Fields
    ? Schema.Codec<Schema.Struct<I>["Type"], Schema.Struct<I>["Encoded"], never, never>
    : never;

/** A task with no input. */
export function task<A, E = never, R = never>(
  handler: () => Effect.Effect<A, E, R>,
): TaskDef<Schema.Codec<{}, {}, never, never>, undefined, A, E, R>;
/** A task with an output schema: the handler returns its `Type`; its `Encoded` is stored. */
export function task<const I extends TaskInputSpec, Out extends TaskSchema, E = never, R = never>(
  input: I,
  options: { readonly output: Out; readonly retry?: RetryPolicy },
  handler: (input: ToSchema<I>["Type"]) => Effect.Effect<Out["Type"], E, R>,
): TaskDef<ToSchema<I>, Out, Out["Type"], E, R>;
/** A task with options other than an output schema. */
export function task<const I extends TaskInputSpec, A, E = never, R = never>(
  input: I,
  options: { readonly retry?: RetryPolicy },
  handler: (input: ToSchema<I>["Type"]) => Effect.Effect<A, E, R>,
): TaskDef<ToSchema<I>, undefined, A, E, R>;
/** The common case: input fields (or a schema) and a handler. */
export function task<const I extends TaskInputSpec, A, E = never, R = never>(
  input: I,
  handler: (input: ToSchema<I>["Type"]) => Effect.Effect<A, E, R>,
): TaskDef<ToSchema<I>, undefined, A, E, R>;
export function task(...args: Array<unknown>): unknown {
  const [input, options, handler] =
    args.length === 1 ? [{}, {}, args[0]] : args.length === 2 ? [args[0], {}, args[1]] : [args[0], args[1], args[2]];
  const schema = Schema.isSchema(input) ? input : Schema.Struct(input as Schema.Struct.Fields);
  const opts = options as { readonly output?: TaskSchema; readonly retry?: RetryPolicy };
  return {
    [TaskDefTypeId]: TaskDefTypeId,
    input: schema,
    output: opts.output,
    retry: opts.retry,
    handler: args.length === 1 ? () => (handler as () => unknown)() : handler,
  };
}

// ---------------------------------------------------------------------------
// LLM agents: Agent.prompt and Agent.tools
// ---------------------------------------------------------------------------

/**
 * Runs one LLM turn and returns its answer: text, or a value of `output`'s type. Only in agents
 * with a `model`. The turn is durable: the task waits (holding no execution) while it runs, then
 * the handler runs again from the top and this call returns the stored answer. So code before a
 * prompt can run more than once; keep it idempotent.
 */
export function prompt(
  message: string,
  options?: { readonly session?: string },
): Effect.Effect<string, PromptFailed, Prompting>;
export function prompt<Output extends TaskSchema>(
  message: string,
  options: { readonly session?: string; readonly output: Output },
): Effect.Effect<Output["Type"], PromptFailed, Prompting>;
export function prompt(
  message: string,
  options: { readonly session?: string; readonly output?: TaskSchema } = {},
): Effect.Effect<unknown, PromptFailed, Prompting> {
  return Effect.flatMap(Effect.service(promptingKey), (p) => p.run(message, options));
}

/** Services a tool handler may use. */
type ToolServices<Uses extends ReadonlyArray<AgentClass>> = AgentContext | ToolCall | RefsOf<Uses>;

/** Tools for an LLM agent: an Effect AI toolkit and its handlers. `R`: what the handlers need. */
export interface AgentTools<R = never> extends ToolsDef {
  readonly _R?: (_: never) => R;
}

/**
 * `tools: Agent.tools(Toolkit.make(LookupOrder), { lookup_order: ({ orderId }) => ... })`. Unlike
 * plain Effect AI handlers, these may use `AgentContext`, `ToolCall` (for an idempotency key),
 * and the agents in `uses`.
 */
export const tools = <
  Tools extends Record<string, Tool.Any>,
  const Handlers extends {
    readonly [K in keyof Tools]: (
      params: Tool.Parameters<Tools[K]>,
    ) => Effect.Effect<Tool.Success<Tools[K]>, Tool.Failure<Tools[K]>, any>;
  },
>(
  toolkit: Toolkit.Toolkit<Tools>,
  handlers: Handlers,
): AgentTools<HandlerServices<Handlers>> => ({ toolkit, handlers: handlers as never });
type HandlerServices<H> = { [K in keyof H]: H[K] extends (...args: any) => Effect.Effect<any, any, infer R> ? R : never }[keyof H];

// ---------------------------------------------------------------------------
// Agent.make
// ---------------------------------------------------------------------------

export interface AgentOptions<Uses extends ReadonlyArray<AgentClass>> {
  /** Agents this one sends to; handlers can reach only these. */
  readonly uses?: Uses;
  /** Default retry policy for every task; each task can override it. */
  readonly retry?: RetryPolicy;
  /** Makes this an LLM agent: `Agent.prompt` works, and a built-in `prompt` task is added. */
  readonly model?: AgentModel;
  /** The system prompt for every turn. */
  readonly instructions?: string;
  readonly tools?: AgentTools<any>;
  readonly limits?: Limits;
}

/** The built-in task of every LLM agent: one turn in a session (a new one if omitted). */
export type PromptTask = TaskDef<
  Schema.Codec<{ readonly message: string; readonly session?: string }, { readonly message: string; readonly session?: string }, never, never>,
  undefined,
  { text: string },
  PromptFailed,
  Prompting
>;
const promptTask = task({ message: Schema.String, session: Schema.optionalKey(Schema.String) }, ({ message, session }) =>
  Effect.map(prompt(message, session === undefined ? {} : { session }), (text) => ({ text })),
);

/** A definition is either the tasks themselves, or `{ ...options, tasks }`. */
type UserTasksOfDef<D> = D extends { readonly tasks: infer T extends AnyTasks } ? T : D;
type HasModel<D> = D extends { readonly model: AgentModel } ? true : false;
type TasksOfDef<D> =
  HasModel<D> extends true
    ? Omit<{ readonly prompt: PromptTask }, keyof UserTasksOfDef<D>> & UserTasksOfDef<D>
    : UserTasksOfDef<D>;
type UsesOfDef<D> = D extends { readonly uses: infer U extends ReadonlyArray<AgentClass> } ? U : readonly [];
type RefsOf<Uses extends ReadonlyArray<AgentClass>> = Uses[number] extends infer U
  ? U extends AgentClass
    ? Reach<AgentType<U>>
    : never
  : never;

/**
 * Compile-time checks on each task, reported against its name: not a reserved name; needs only
 * framework services and the agents in `uses` (and doesn't wait on them, until M3); without an
 * `output` schema, returns plain JSON (or nothing).
 */
type CheckTasks<Tasks extends AnyTasks, Allowed> = {
  [K in keyof Tasks]: K extends ReservedTaskName
    ? `"${K & string}" can't be a task name (reserved by agent references)`
    : [Exclude<ServicesOf<Tasks[K]>, Allowed>] extends [never]
      ? [RawOutputOf<Tasks[K]>] extends [Json | void | undefined]
        ? Tasks[K]
        : `Task "${K & string}" returns a value that isn't plain JSON; add an output schema`
      : [Extract<ServicesOf<Tasks[K]>, Waiter>] extends [never]
        ? [Extract<ServicesOf<Tasks[K]>, Prompting>] extends [never]
          ? `Task "${K & string}" needs a service the framework does not provide`
          : `Task "${K & string}" uses Agent.prompt, but the agent has no model`
        : `Task "${K & string}" waits for another agent; use send.<task> until M3`;
};

type ToolsServicesOf<D> = D extends { readonly tools: AgentTools<infer R> } ? R : never;
type CheckTools<D> = [Exclude<ToolsServicesOf<D>, ToolServices<UsesOfDef<D>>>] extends [never]
  ? {}
  : { readonly tools: "A tool handler needs a service the framework does not provide" };

type CheckDef<D> = D extends { readonly tasks: infer T extends AnyTasks }
  ? Omit<D, "tasks"> & {
      readonly tasks: CheckTasks<
        T,
        AgentContext | RefsOf<UsesOfDef<D>> | (HasModel<D> extends true ? Prompting : never)
      >;
    } & CheckTools<D>
  : D extends AnyTasks
    ? CheckTasks<D, AgentContext>
    : never;

export interface AgentStatics<Name extends string, Tasks extends AnyTasks> {
  readonly definition: AgentDefinition<Name, Tasks>;
  /** A typed reference to the agent with this name. */
  named<This>(this: This, name: string): AgentRef<This>;
}

/**
 * `class Greeter extends Agent.make<Greeter>()("Greeter", { greet: Agent.task(...) }) {}`, or with
 * options: `Agent.make<Relay>()("Relay", { uses: [Greeter], retry, tasks: { ... } })`.
 * The double call mirrors `Cloudflare.DurableObject<Self>()`: `Self` is explicit, the rest inferred.
 */
export const make =
  <Self>() =>
  <
    const Name extends string,
    const D extends AnyTasks | (AgentOptions<ReadonlyArray<AgentClass>> & { readonly tasks: AnyTasks }),
  >(
    name: Name,
    definition: D & CheckDef<D>,
  ) => {
    const def = definition as unknown as Record<string, unknown>;
    const isOptions = "tasks" in def && !isTaskDef(def.tasks);
    const options = (isOptions ? def : {}) as AgentOptions<ReadonlyArray<AgentClass>>;
    const userTasks = (isOptions ? def.tasks : def) as AnyTasks;
    const tasks: AnyTasks =
      options.model !== undefined && !("prompt" in userTasks) ? { prompt: promptTask, ...userTasks } : userTasks;

    const base = Cloudflare.DurableObject<Self>()(
      name,
      agentObject({
        name,
        tasks,
        retry: options.retry,
        uses: options.uses ?? [],
        model: options.model,
        instructions: options.instructions,
        tools: options.tools,
        limits: options.limits,
      }),
    );
    const statics: AgentStatics<Name, TasksOfDef<D>> = {
      definition: { name, tasks: tasks as TasksOfDef<D>, retry: options.retry },
      named(this: unknown, agentName: string) {
        return makeRef(this as AgentClass, agentName) as never;
      },
    };
    return Object.assign(base, statics);
  };

// ---------------------------------------------------------------------------
// Agent.connect
// ---------------------------------------------------------------------------

/** The Context that lets references reach `agents` through their Durable Object bindings. */
export const bindings = <const Agents extends ReadonlyArray<AgentClass>>(agents: Agents) =>
  Effect.gen(function* () {
    let context = Context.make(waiterKey, true as const);
    for (const agent of agents) {
      const namespace = (yield* (agent as unknown as Effect.Effect<Namespace>)) as Namespace;
      context = Context.add(context, reachKey(agent.definition.name), {
        transport: (n: string) => bindingTransport(namespace, n, null),
      });
    }
    return context as unknown as Context.Context<Reach<AgentType<Agents[number]>> | Waiter>;
  });

/**
 * In a Worker's construction phase: binds the agents (this Worker hosts their Durable Objects) and
 * returns a Layer that lets code reach them and wait. `fetch: handler.pipe(Effect.provide(agents))`.
 */
export const connect = <const Agents extends ReadonlyArray<AgentClass>>(...agents: Agents) =>
  Effect.map(bindings(agents), (context) => Layer.succeedContext(context));
