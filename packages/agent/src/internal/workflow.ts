// Agent calls inside an `effect/workflow` workflow (spike 0.4, the M5 foundation). Engine-agnostic:
// it uses only `Activity`, `DurableDeferred`, and `WorkflowEngine.deferredDone`, so it runs the same
// on Alchemy's Durable Object engine and on its native Cloudflare Workflows engine.
//
// The n-th agent call of a run (`c<n>`; a run replays from the top, so positions repeat):
//   1. Activity `agent-call/c<n>`: enqueue the task (task id `wf/<Workflow>/<executionId>/c<n>`, or
//      the Rpc's primaryKey) and, for a waiting call, `subscribe` with reply-to
//      `workflow:<Workflow>` / `<executionId>` / `agent-call/c<n>`. Its result is the record if the
//      task had already settled, else null. Stored by the engine, so it runs once per run's life.
//   2. `DurableDeferred.await(agent-call/c<n>)`: the agent's outbox delivers the reply through
//      the engine's `deferredDone` (see `signalWorkflow` below, used by `outbox.ts`).
import { CurrentActivity } from "alchemy/WorkflowEncoding";
import type * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Activity, DurableDeferred, type Workflow, WorkflowEngine } from "effect/workflow";
import { callAgent } from "./client.ts";
import { AgentError, type Address, type CallRequest } from "./core.ts";
import { TaskRecord } from "./inbox.ts";

/** Reply-to type prefix for a workflow run (the subscriber's "agent type"). */
export const WORKFLOW_PREFIX = "workflow:";

/** Calls made so far by one run. A run gets a fresh `WorkflowInstance`, so this restarts with it. */
const counters = new WeakMap<object, { n: number }>();

/** The workflow run this code is in, if any. Inside one of its activities, a client call dies. */
export const currentRun = Effect.gen(function* () {
  const instance = yield* Effect.serviceOption(WorkflowEngine.WorkflowInstance);
  const engine = yield* Effect.serviceOption(WorkflowEngine.WorkflowEngine);
  if (Option.isNone(instance) || Option.isNone(engine)) return undefined;
  // An activity runs once and its result is journaled: an agent call inside it would be a step
  // inside a step. Calls belong in the workflow body, where each one is its own step.
  if (Option.isSome(yield* Effect.serviceOption(CurrentActivity))) {
    return yield* Effect.die(
      new Error(`an agent call inside an Activity of ${instance.value.workflow._tag}: call agents from the workflow body, where each call is a durable step`),
    );
  }
  return { instance: instance.value, engine: engine.value };
});

type Run = NonNullable<Effect.Success<typeof currentRun>>;

/** What the send step records: which call it was (checked on replay), and the record if already settled. */
const Sent = Schema.Struct({ call: Schema.String, record: Schema.NullOr(TaskRecord) });

/** One client call inside a workflow run: a durable step that waits without holding execution. */
export const workflowCall = (run: Run, request: CallRequest): Effect.Effect<TaskRecord | undefined, AgentError> => {
  const { instance, engine } = run;
  const counter = counters.get(instance) ?? { n: 0 };
  counters.set(instance, counter);
  const id = `c${counter.n++}`;
  const workflowName = instance.workflow._tag;
  const taskId = request.taskId ?? `wf/${workflowName}/${instance.executionId}/${id}`;
  const name = `agent-call/${id}`;
  const deferred = DurableDeferred.make(name, { success: TaskRecord });
  const agent: Address = request.agent;
  const causation = `workflow/${workflowName}/${instance.executionId}`;

  const call = `${agent.type}/${agent.name}.${request.tag}${request.wait ? "" : " (discard)"}`;
  const send = Activity.make({
    name,
    success: Sent,
    error: AgentError,
    execute: Effect.gen(function* () {
      const recorded = yield* callAgent(agent, (stub) => stub.enqueue(taskId, request.tag, request.payload, { causation }));
      if (!request.wait) return { call, record: null };
      if (recorded.status === "completed" || recorded.status === "failed") return { call, record: recorded };
      const record = yield* callAgent(agent, (stub) =>
        stub.subscribe(taskId, { type: `${WORKFLOW_PREFIX}${workflowName}`, name: instance.executionId, id: name }),
      );
      return { call, record: record !== null && (record.status === "completed" || record.status === "failed") ? record : null };
    }),
  });

  return Effect.gen(function* () {
    const sent = yield* send;
    // Steps are journaled by position: a run whose code now makes a different call here would
    // otherwise get another call's answer (for example after a deploy that reordered the calls).
    if (sent.call !== call) {
      return yield* Effect.die(
        new Error(`agent call #${id.slice(1)} was ${sent.call} when this run started, and is ${call} now. A workflow must make the same calls in the same order.`),
      );
    }
    if (!request.wait) return undefined;
    if (sent.record !== null) return sent.record;
    return yield* DurableDeferred.await(deferred);
  }).pipe(
    Effect.provideService(WorkflowEngine.WorkflowInstance, instance),
    Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
  ) as Effect.Effect<TaskRecord | undefined, AgentError>;
};

/**
 * Delivers an agent's reply to a waiting workflow run (from the agent's outbox). Idempotent:
 * both engines take a deferred's first completion only.
 */
export const signalWorkflow = (
  engine: Context.Service.Shape<typeof WorkflowEngine.WorkflowEngine>,
  target: { readonly type: string; readonly name: string },
  deferredName: string,
  record: TaskRecord,
) =>
  engine.deferredDone(DurableDeferred.make(deferredName, { success: TaskRecord }), {
    workflowName: target.type.slice(WORKFLOW_PREFIX.length),
    executionId: target.name,
    deferredName,
    exit: Exit.succeed(record),
  });

// ---------------------------------------------------------------------------
// Hosting: the engine, wrapped to remember which workflows this Worker runs (for the HTTP API)
// ---------------------------------------------------------------------------

/** Workflows registered with this Worker's engine, by name. */
export const registry = new Map<string, Workflow.Any>();

/** The engine, recording each workflow that registers with it. */
export const trackRegistrations = <E, R>(
  engine: Layer.Layer<WorkflowEngine.WorkflowEngine, E, R>,
): Layer.Layer<WorkflowEngine.WorkflowEngine, E, R> =>
  Layer.effect(
    WorkflowEngine.WorkflowEngine,
    Effect.map(Effect.service(WorkflowEngine.WorkflowEngine), (inner) => ({
      ...inner,
      register: (workflow, execute) =>
        Effect.andThen(
          Effect.sync(() => void registry.set(workflow._tag, workflow)),
          inner.register(workflow, execute),
        ),
    })),
  ).pipe(Layer.provide(engine));
