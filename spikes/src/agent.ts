// Spike 0.2: a throwaway `Agent` module. It answers "how far does typing stretch", not "how
// should the inbox work". There is no inbox: `enqueue` runs the handler immediately and stores
// the outcome under `task:<taskId>`.
import * as Cloudflare from "@alchemy.run/cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

// ---------------------------------------------------------------------------
// Framework-provided services and errors
// ---------------------------------------------------------------------------

export class AgentContext extends Context.Service<
  AgentContext,
  {
    readonly agent: { readonly definition: string; readonly name: string };
    readonly taskId: string;
  }
>()("alchemy-agent/AgentContext") {}

/**
 * Every service a task handler may require. Anything else is a compile error at `Agent.make`.
 * `RuntimeContext` is Alchemy's: calling another agent's stub requires it.
 */
export type FrameworkServices = AgentContext | RuntimeContext;

export class PermanentError extends Schema.TaggedError<PermanentError>()("PermanentError", {
  message: Schema.String,
}) {}

export class TaskInputError extends Schema.TaggedError<TaskInputError>()("TaskInputError", {
  task: Schema.String,
  message: Schema.String,
}) {}

export class UnknownTaskError extends Schema.TaggedError<UnknownTaskError>()("UnknownTaskError", {
  task: Schema.String,
}) {}

// ---------------------------------------------------------------------------
// Task definitions
// ---------------------------------------------------------------------------

/** Task schemas must decode and encode without services: they run on both sides of the RPC. */
export type TaskSchema = Schema.Codec<any, any, never, never>;

export interface TaskDef<
  In extends TaskSchema,
  Out extends TaskSchema | undefined,
  A,
  E,
  R,
> {
  readonly input: In;
  readonly output: Out;
  readonly handler: (input: In["Type"]) => Effect.Effect<A, E, R>;
}

// `any` for the input schema: `TaskDef` is invariant in it (it appears in the handler parameter).
export type AnyTaskDef = TaskDef<any, any, any, any, any>;

/** A task with a declared `output` schema: the handler must return the schema's `Type`. */
export function task<In extends TaskSchema, Out extends TaskSchema, E = never, R = never>(
  spec: { readonly input: In; readonly output: Out },
  handler: (input: In["Type"]) => Effect.Effect<Out["Type"], E, R>,
): TaskDef<In, Out, Out["Type"], E, R>;
/** A task whose output type is inferred from the handler. */
export function task<In extends TaskSchema, A, E = never, R = never>(
  spec: { readonly input: In },
  handler: (input: In["Type"]) => Effect.Effect<A, E, R>,
): TaskDef<In, undefined, A, E, R>;
export function task(
  spec: { readonly input: TaskSchema; readonly output?: TaskSchema },
  handler: (input: unknown) => Effect.Effect<unknown, unknown, unknown>,
): AnyTaskDef {
  return { input: spec.input, output: spec.output, handler };
}

// ---------------------------------------------------------------------------
// Helper types. All of them read the definition, never Alchemy's RPC types.
// ---------------------------------------------------------------------------

export interface AgentDefinition<Name extends string, Tasks extends Record<string, AnyTaskDef>> {
  readonly name: Name;
  readonly tasks: Tasks;
}

type DefOf<Ag> = Ag extends { readonly definition: infer D extends AgentDefinition<any, any> }
  ? D
  : never;
type TasksOf<Ag> = DefOf<Ag>["tasks"];

export type TaskNames<Ag> = keyof TasksOf<Ag> & string;
/** What a caller passes: the input schema's encoded (JSON-safe) form. The default. */
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

/**
 * A failure as it is stored and as it crosses RPC: the tag and message survive, class identity
 * does not. A defect (a thrown error or `Effect.die`) is stored as `Defect`.
 */
export type TaskFailure<E> =
  | (E extends { readonly _tag: infer Tag extends string }
      ? { readonly _tag: Tag; readonly message: string }
      : never)
  | { readonly _tag: "Defect"; readonly message: string };

export type TaskRecord<O, E> =
  | { readonly status: "completed"; readonly task: string; readonly result: O }
  | { readonly status: "failed"; readonly task: string; readonly error: TaskFailure<E> };

/** Union of every task's record. `result(taskId)` can't know which task an id belongs to. */
type AnyTaskRecord<Tasks extends Record<string, AnyTaskDef>> = {
  [K in keyof Tasks]: TaskRecord<OutputOf<Tasks[K]>, ErrorOf<Tasks[K]>>;
}[keyof Tasks];

/**
 * Compile-time check that every handler only needs framework services. Fails with a readable
 * message naming the task, instead of a deep assignability error.
 */
type CheckServices<Tasks extends Record<string, AnyTaskDef>, Allowed> = {
  [K in keyof Tasks]: [Exclude<ServicesOf<Tasks[K]>, Allowed>] extends [never]
    ? Tasks[K]
    : `Task "${K & string}" needs a service the framework does not provide`;
};

// ---------------------------------------------------------------------------
// The DO's RPC surface (approach A: one generic method)
// ---------------------------------------------------------------------------

export interface TaskHandle<K extends string = string> {
  readonly taskId: string;
  readonly task: K;
}

// A `type`, not an `interface`: Alchemy's `MainRpc` constraint is an index signature, and
// interfaces aren't assignable to index signatures.
export type AgentShape<Tasks extends Record<string, AnyTaskDef>> = {
  enqueue: <K extends keyof Tasks & string>(
    taskId: string,
    task: K,
    input: Tasks[K]["input"]["Encoded"],
  ) => Effect.Effect<TaskHandle<K>, TaskInputError | UnknownTaskError, RuntimeContext>;
  result: (taskId: string) => Effect.Effect<AnyTaskRecord<Tasks> | undefined, never, RuntimeContext>;
};

const toFailure = (error: unknown): { _tag: string; message: string } =>
  typeof error === "object" && error !== null && "_tag" in error
    ? {
        _tag: String((error as { _tag: unknown })._tag),
        message: String((error as { message?: unknown }).message ?? ""),
      }
    : { _tag: "Defect", message: error instanceof Error ? error.message : String(error) };

// ---------------------------------------------------------------------------
// Agent.make
// ---------------------------------------------------------------------------

/**
 * `class Echo extends Agent.make<Echo>()("Echo", { tasks }) {}`. The double call mirrors
 * `Cloudflare.DurableObject<Self>()`: `Self` must be passed explicitly and the rest inferred,
 * and TypeScript can't mix the two in one call.
 *
 * `uses` lists the other agents this one calls. Each is bound in the DO's construction phase
 * (Alchemy only allows bindings there), and handlers reach it with `yield* Agent.use(Other)`.
 */
export const make =
  <Self>() =>
  <
    const Name extends string,
    Tasks extends Record<string, AnyTaskDef>,
    const Uses extends ReadonlyArray<AgentClass> = readonly [],
  >(
    name: Name,
    definition: {
      readonly uses?: Uses;
      readonly tasks: Tasks & CheckServices<Tasks, FrameworkServices | RefsOf<Uses>>;
    },
  ) => {
    const tasks = definition.tasks as Record<string, AnyTaskDef>;
    const uses = (definition.uses ?? []) as ReadonlyArray<AgentClass>;
    const base = Cloudflare.DurableObject<Self>()(
      name,
      Effect.gen(function* () {
        const state = yield* Cloudflare.DurableObjectState;
        // Construction phase: bind every agent this one uses.
        const refs: Array<readonly [Context.Key<any, any>, AgentLookup<any>]> = [];
        for (const other of uses) {
          const namespace = (yield* other) as {
            getByName: (name: string) => unknown;
          };
          refs.push([
            refKey(other.definition.name),
            { getByName: (n: string) => client(other, namespace.getByName(n) as UntypedStub) },
          ]);
        }
        return Effect.gen(function* () {
          const agentName = state.id.name ?? state.id.toString();
          const key = (taskId: string) => `task:${taskId}`;

          // `CheckServices` guarantees at `make` that handlers only need `AgentContext` and the
          // refs in `uses`, all provided below; the erased `AnyTaskDef` can't show that.
          const provideFramework = (taskId: string) =>
            <A, E>(effect: Effect.Effect<A, E, any>): Effect.Effect<A, E> =>
              refs
                .reduce(
                  (acc, [k, v]) => Effect.provideService(acc, k, v),
                  effect as Effect.Effect<A, E, any>,
                )
                .pipe(
                  Effect.provideService(AgentContext, {
                    agent: { definition: name, name: agentName },
                    taskId,
                  }),
                ) as Effect.Effect<A, E>;

          const run = (taskId: string, taskName: string, def: AnyTaskDef, input: unknown) =>
            def.handler(input).pipe(
              provideFramework(taskId),
              Effect.flatMap((value) =>
                def.output === undefined
                  ? Effect.succeed(value)
                  : Schema.encodeUnknownEffect(def.output)(value).pipe(Effect.orDie),
              ),
              Effect.map((result) => ({ status: "completed" as const, task: taskName, result })),
              Effect.catchCause((cause) => {
                const [first] = cause.reasons;
                const error =
                  first === undefined
                    ? { _tag: "Defect", message: "interrupted" }
                    : first._tag === "Fail"
                      ? toFailure(first.error)
                      : first._tag === "Die"
                        ? { _tag: "Defect", message: toFailure(first.defect).message }
                        : { _tag: "Defect", message: "interrupted" };
                return Effect.succeed({ status: "failed" as const, task: taskName, error });
              }),
            );

          const shape: AgentShape<Tasks> = {
            enqueue: (taskId, taskName, input) =>
              Effect.gen(function* () {
                const def = tasks[taskName];
                if (def === undefined) return yield* new UnknownTaskError({ task: taskName });
                // Same task id twice is a no-op (the task id is the idempotency key).
                const existing = yield* state.storage.get(key(taskId));
                if (existing !== undefined) return { taskId, task: taskName };
                const decoded = yield* Schema.decodeUnknownEffect(def.input)(input).pipe(
                  Effect.mapError(
                    (issue) => new TaskInputError({ task: taskName, message: issue.message }),
                  ),
                );
                const record = yield* run(taskId, taskName, def, decoded);
                yield* state.storage.put(key(taskId), record);
                return { taskId, task: taskName };
              }),
            result: (taskId) =>
              state.storage
                .get<AnyTaskRecord<Tasks>>(key(taskId))
                .pipe(Effect.orDie) as Effect.Effect<
                AnyTaskRecord<Tasks> | undefined,
                never,
                RuntimeContext
              >,
          };
          return shape;
        });
      }),
    );
    return Object.assign(base, {
      definition: { name, tasks: definition.tasks as Tasks } as AgentDefinition<Name, Tasks>,
    });
  };

// ---------------------------------------------------------------------------
// Agent references (one agent calling another)
// ---------------------------------------------------------------------------

/** The service a handler needs to reach agent `Name`. Only `uses` can provide it. */
export interface AgentRef<Name extends string> {
  readonly _AgentRef: Name;
}

export interface AgentLookup<Ag> {
  readonly getByName: (name: string) => AgentClient<Ag>;
}

/** Any class made by `Agent.make`: a static definition plus Alchemy's binding Effect. */
export type AgentClass = AnyAgent & Effect.Effect<unknown, never, any>;

type RefsOf<Uses extends ReadonlyArray<AgentClass>> = Uses[number] extends infer U
  ? U extends AnyAgent
    ? AgentRef<U["definition"]["name"]>
    : never
  : never;

const refKeys = new Map<string, Context.Key<any, any>>();
const refKey = (name: string): Context.Key<any, any> => {
  let k = refKeys.get(name);
  if (k === undefined) {
    k = Context.Service<any, any>(`alchemy-agent/AgentRef/${name}`);
    refKeys.set(name, k);
  }
  return k;
};

/** Inside a handler: `const echo = yield* Agent.use(Echo)`, then `echo.getByName("a").send(...)`. */
export const use = <Ag extends AnyAgent>(
  agent: Ag,
): Effect.Effect<AgentLookup<Ag>, never, AgentRef<Ag["definition"]["name"]>> =>
  Effect.service(refKey(agent.definition.name)) as never;

// ---------------------------------------------------------------------------
// Approach C: a client typed from the definition, wrapping any stub
// ---------------------------------------------------------------------------

type AnyAgent = { readonly definition: AgentDefinition<string, Record<string, AnyTaskDef>> };

/** The minimal stub surface the client needs; any agent's `getByName(...)` result fits. */
interface UntypedStub {
  enqueue: (
    taskId: string,
    task: string,
    input: unknown,
  ) => Effect.Effect<TaskHandle, TaskInputError | UnknownTaskError, RuntimeContext>;
  result: (taskId: string) => Effect.Effect<unknown, never, RuntimeContext>;
}

export interface AgentClient<Ag> {
  send<K extends TaskNames<Ag>>(
    taskId: string,
    task: K,
    input: TaskInput<Ag, K>,
  ): Effect.Effect<TaskHandle<K>, TaskInputError | UnknownTaskError, RuntimeContext>;
  result<K extends TaskNames<Ag>>(
    handle: TaskHandle<K>,
  ): Effect.Effect<TaskRecord<TaskOutput<Ag, K>, TaskError<Ag, K>> | undefined, never, RuntimeContext>;
}

/** Any agent stub: its typed `enqueue` is generic, so it can't be assigned to `UntypedStub`. */
type AnyStub = { enqueue: (...args: any[]) => unknown; result: (...args: any[]) => unknown };

export const client = <Ag extends AnyAgent>(agent: Ag, anyStub: AnyStub): AgentClient<Ag> => {
  const stub = anyStub as UntypedStub;
  return {
  send: (taskId, task, input) =>
    Effect.gen(function* () {
      const def = agent.definition.tasks[task];
      if (def === undefined) return yield* new UnknownTaskError({ task });
      // Validate before the round trip; the DO validates again.
      yield* Schema.decodeUnknownEffect(def.input)(input).pipe(
        Effect.mapError((issue) => new TaskInputError({ task, message: issue.message })),
      );
      return (yield* stub.enqueue(taskId, task, input)) as TaskHandle<typeof task>;
    }),
  result: (handle) =>
    Effect.gen(function* () {
      const record = (yield* stub.result(handle.taskId)) as
        | { status: "completed"; task: string; result: unknown }
        | { status: "failed"; task: string; error: unknown }
        | undefined;
      const def = agent.definition.tasks[handle.task];
      if (record?.status !== "completed" || def?.output === undefined) return record as never;
      // Stored results are encoded; decode with the output schema when there is one.
      const result = yield* Schema.decodeUnknownEffect(def.output)(record.result).pipe(
        Effect.orDie,
      );
      return { ...record, result } as never;
    }),
};
};
