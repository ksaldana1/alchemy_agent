// The agent Durable Object: inbox, drain loop, durable generation, and the RPC surface clients
// call. See docs/ARCHITECTURE.md ("The inbox", "LLM turns") for the data flow.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Tool from "effect/ai/Tool";
import type * as Toolkit from "effect/ai/Toolkit";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as SchemaAST from "effect/SchemaAST";
import type { Rpc } from "effect/rpc";
import type { AgentContract, AgentRpc } from "./client.ts";
import {
  AgentContext,
  AgentErrorReason,
  Calls,
  CurrentExecution,
  ExecutionLost,
  ExternalTask,
  FsDenied,
  GenerationError,
  GenerationFailed,
  Generating,
  InvalidPayload,
  ModelError,
  Retry,
  RetriesExhausted,
  Suspend,
  UnknownTask,
  type Address,
  type GenerateRequest,
  type RetryPolicy,
} from "./core.ts";
import * as Inbox from "./inbox.ts";
import { makeOutbox, type Outbox } from "./outbox.ts";
import { makeSessions, type Limits, type OutputSpec } from "./session.ts";
import { signalWorkflow } from "./workflow.ts";
import { reportSeen } from "./registry.ts";
import { WorkflowEngine } from "effect/workflow";
import { DeferredModel } from "../AgentModel.ts";

const DEFAULT_RETRY = { maxAttempts: 3, delay: "1 second", maxDelay: "5 minutes" } as const;
/** Tasks run per drain pass before it reschedules itself, so one pass stays short. */
const DRAIN_BATCH = 10;
const DRAIN_CALLBACK = "alchemy-agent/drain";
const STEP_CALLBACK = "alchemy-agent/step";
const OUTBOX_CALLBACK = "alchemy-agent/outbox";
const WAKE_CALLBACK = "alchemy-agent/wake";

export type Handler = (request: {
  readonly payload: unknown;
  readonly taskId: string;
  readonly attempt: number;
  readonly agent: Address;
}) => Effect.Effect<unknown, unknown, unknown>;

type Outcome =
  | { readonly _tag: "Completed"; readonly result: unknown }
  | { readonly _tag: "Suspended" }
  | { readonly _tag: "Failed"; readonly permanent: boolean; readonly error: Inbox.StoredFailure };
/** An execution's outcome, and the calls it made (recorded only if it completed or suspended). */
type Executed = { readonly outcome: Outcome; readonly flush: Effect.Effect<void, never, RuntimeContext> };

const json = (schema: Schema.Top) => Schema.toCodecJson(schema as Schema.Codec<unknown, unknown, never, never>);
const encodeReason = Schema.encodeSync(json(AgentErrorReason));
const agentFailure = (reason: typeof AgentErrorReason.Type): Inbox.StoredFailure => ({ kind: "agent", reason: encodeReason(reason) });
const messageOf = (e: unknown): string =>
  e instanceof Error ? e.message : Predicate.hasProperty(e, "message") ? String(e.message) : String(e);

export interface HostConfig {
  readonly contract: AgentContract;
  /** The handlers, or the construction Effect that builds them. */
  readonly build: Record<string, Handler> | Effect.Effect<Record<string, Handler>, never, any>;
  readonly retry: RetryPolicy | undefined;
  readonly toolkit: Toolkit.Any | undefined;
  readonly limits: Limits | undefined;
}

/** How a generate call's answer is produced and checked (stored with the turn). */
const outputSpecOf = (schema: Schema.Codec<any, any, never, never> | undefined): OutputSpec => {
  if (schema === undefined) return { mode: "text" };
  if (SchemaAST.isString(SchemaAST.toEncoded(schema.ast))) {
    return { mode: "text", document: Schema.toJsonSchemaDocument(schema) };
  }
  return { mode: "respond", tool: Tool.getJsonSchemaFromSchema(schema), document: Schema.toJsonSchemaDocument(schema) };
};

/** The construction Effect for the agent's Durable Object class. */
export const agentObject = (config: HostConfig) => {
  const { contract } = config;
  const type = contract.type;
  // The agent's policy, overridden per task by an `Agent.Retry` annotation on its Rpc.
  const policyOf = (tag: string) => {
    const rpc = contract.rpcs.get(tag);
    const p = { ...DEFAULT_RETRY, ...config.retry, ...(rpc ? Context.get(rpc.annotations, Retry) : undefined) };
    return {
      maxAttempts: p.maxAttempts,
      delayMs: Duration.toMillis(Duration.fromInputUnsafe(p.delay)),
      maxDelayMs: Duration.toMillis(Duration.fromInputUnsafe(p.maxDelay)),
    };
  };
  const backoff = (tag: string, attempts: number) => {
    const p = policyOf(tag);
    return Math.min(p.maxDelayMs, p.delayMs * 2 ** Math.max(0, attempts - 1));
  };

  return Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    // Everything the implementation was given with `Layer.provide` (a model, tool handlers, the
    // user's own services) is in this context; handlers and steps run with it.
    const context = yield* Effect.context<never>();
    const handlers = (Effect.isEffect(config.build) ? yield* config.build : config.build) as Record<string, Handler>;
    // A model is either a plain LanguageModel or one built inside each step (AgentModel.workersAI).
    const deferredModel = Context.getOption(context, DeferredModel);
    const hasModel = Option.isSome(deferredModel) || Option.isSome(Context.getOption(context, LanguageModel.LanguageModel));
    const provideModel = <A, E>(effect: Effect.Effect<A, E, LanguageModel.LanguageModel>) =>
      Option.isSome(deferredModel)
        ? (Effect.provide(effect, deferredModel.value.layer) as Effect.Effect<A, E, RuntimeContext>)
        : (Effect.provideContext(effect, context as Context.Context<LanguageModel.LanguageModel>) as Effect.Effect<A, E>);

    return Effect.gen(function* () {
      const sql = state.storage.sql as unknown as Inbox.Sql;
      const transaction = <A>(effect: Effect.Effect<A, never, RuntimeContext>) =>
        state.storage.transaction(effect) as Effect.Effect<A, never, RuntimeContext>;
      yield* Inbox.migrate(sql, transaction);
      const agent: Address = { type, name: state.id.name ?? state.id.toString() };
      const now = () => Date.now();

      const execution = {
        abort: (reason: string) => state.raw.abort(reason),
        // Synced before returning, so the count survives an abort right after (tests crash on it).
        count: (counter: string) =>
          Inbox.count(sql, counter).pipe(Effect.tap(() => state.storage.sync())) as Effect.Effect<number>,
      };

      // In-memory waiters for `wait`: released after a task's settling transaction commits.
      const waiters = new Map<string, Array<Deferred.Deferred<void>>>();
      const release = (taskId: string) =>
        Effect.suspend(() => {
          const ds = waiters.get(taskId) ?? [];
          waiters.delete(taskId);
          return Effect.forEach(ds, (d) => Deferred.succeed(d, undefined), { discard: true });
        });

      let drain: Alchemy.Callback<{ readonly v: 1 }>;
      const scheduleDrain = (at: number, progress?: number) =>
        drain
          .schedule("drain", { at: Math.max(at, now()), payload: { v: 1 }, ...(progress === undefined ? {} : { progress }) })
          .pipe(Effect.orDie);

      let stepCallback: Alchemy.Callback<{ readonly v: 1; readonly turnId: string }> | undefined;
      let outboxCallback: Alchemy.Callback<{ readonly v: 1 }> | undefined;
      let wakeCallback: Alchemy.Callback<{ readonly v: 1; readonly taskId: string }> | undefined;
      /** M7: resume a waiting task at `at` (job id per task, so a rerun replaces it). */
      const scheduleWake = (taskId: string, at: number) =>
        Effect.flatMap(Inbox.advanceCursor(sql), (progress) =>
          wakeCallback!.schedule(`wake:${taskId}`, { at: Math.max(at, now()), payload: { v: 1, taskId }, progress }).pipe(Effect.orDie),
        );
      // Created below; `callsFor` and `wake` only run once the object is up.
      let outbox: Outbox;
      const causationOf = (taskId: string) => `${type}/${agent.name}/${taskId}`;
      const sessions = makeSessions({
        sql,
        transaction,
        now,
        context,
        provideModel,
        toolkit: config.toolkit,
        limits: config.limits,
        execution,
        taskServices: (taskId) =>
          Inbox.get(sql, taskId).pipe(
            Effect.map((row) => Context.make(AgentContext, { agent, taskId, attempt: row?.attempts ?? 1 })),
          ),
        scheduleStep: (turnId, afterMs, progress) =>
          stepCallback!.schedule(turnId, { after: `${afterMs} millis`, payload: { v: 1, turnId }, progress }).pipe(Effect.orDie),
        onSettled: (taskId) => resumeTask(taskId),
        callsFor: (turn, toolCallId) =>
          outbox.calls({ kind: "turn", id: turn.id, prefix: `${turn.id}/${toolCallId}`, causation: causationOf(turn.task_id) }),
      });
      // With `progress`, the drain doesn't inherit the scheduling job's attempt count (and backoff).
      function resumeTask(taskId: string) {
        return Effect.gen(function* () {
          if (yield* Inbox.resume(sql, taskId, now())) yield* scheduleDrain(now(), yield* Inbox.advanceCursor(sql));
        });
      }
      outbox = makeOutbox({
        sql,
        transaction,
        now,
        self: agent,
        schedule: (at, progress) =>
          outboxCallback!.schedule("outbox", { at: Math.max(at, now()), payload: { v: 1 }, progress }).pipe(Effect.orDie),
        wake: (owner) => (owner.kind === "task" ? resumeTask(owner.id) : sessions.resume(owner.id)),
        // A workflow engine is in the context when the Worker provides one (and the workflows).
        signalWorkflow: (target, deferredName, record) =>
          Option.match(Context.getOption(context, WorkflowEngine.WorkflowEngine), {
            onNone: () => Effect.fail(new Error(`no workflow engine is hosted with ${type}: can't reply to ${target.type}`)),
            onSome: (engine) => signalWorkflow(engine, target, deferredName, record) as Effect.Effect<void>,
          }),
      });

      /** `Agent.generate` in a handler: the i-th call of a task is turn `<taskId>#<i>`. */
      const generating = (row: Inbox.InboxRow) => {
        let index = 0;
        return Generating.of({
          run: (request: GenerateRequest) =>
            Effect.gen(function* () {
              const turnId = `${row.task_id}#${index++}`;
              const settled = yield* sessions.outcome(turnId);
              if (settled === undefined) {
                const sessionId = request.session ?? turnId;
                const events = yield* transaction(
                  sessions.createTurn({
                    id: turnId,
                    sessionId,
                    taskId: row.task_id,
                    message: request.prompt,
                    system: request.system,
                    output: outputSpecOf(request.schema),
                    schema: request.schema,
                  }),
                );
                for (const e of events) yield* sessions.emit(sessionId, e);
                return yield* Effect.die(new Suspend(turnId));
              }
              if (settled === "pending") return yield* Effect.die(new Suspend(turnId));
              if (settled.status === "failed") return yield* Effect.fail(settled.error!);
              if (request.schema === undefined) return settled.output;
              return yield* Schema.decodeUnknownEffect(request.schema)(settled.output).pipe(
                Effect.mapError(
                  (issue) => new GenerationError({ reason: new ModelError({ message: `the answer didn't decode: ${issue.message}` }) }),
                ),
              );
            }) as Effect.Effect<unknown, GenerationError>,
        });
      };

      const execute = (row: Inbox.InboxRow, attempt: number): Effect.Effect<Executed, never, RuntimeContext> => {
        const calls = outbox.calls({ kind: "task", id: row.task_id, prefix: row.task_id, causation: causationOf(row.task_id) });
        // M7: a task waiting for something outside (a person) may ask to be resumed at a time.
        let wakeAt: number | null = null;
        const external = ExternalTask.of({
          waitUntil: (at) =>
            Effect.suspend(() => {
              wakeAt = at;
              return Effect.die(new Suspend(row.task_id));
            }),
        });
        // Recorded with the outcome: the calls made, and the wake-up, in the same transaction.
        const flush = Effect.andThen(
          calls.flush,
          Effect.suspend(() => (wakeAt === null ? Effect.void : scheduleWake(row.task_id, wakeAt))),
        );
        return Effect.map(run(row, attempt, calls.service, external), (outcome) => ({ outcome, flush }));
      };

      const run = Effect.fnUntraced(function* (
        row: Inbox.InboxRow,
        attempt: number,
        calls: Context.Service.Shape<typeof Calls>,
        external: Context.Service.Shape<typeof ExternalTask>,
      ) {
        const rpc: Rpc.AnyWithProps | undefined = contract.rpcs.get(row.task);
        const handler = handlers[row.task];
        if (rpc === undefined || handler === undefined) {
          return { _tag: "Failed", permanent: true, error: agentFailure(new UnknownTask({ tag: row.task })) } as Outcome;
        }
        const payload = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(json(rpc.payloadSchema)))(row.input).pipe(Effect.result);
        if (payload._tag === "Failure") {
          return { _tag: "Failed", permanent: true, error: agentFailure(new InvalidPayload({ message: payload.failure.message })) } as Outcome;
        }
        let services = Context.merge(context, Context.make(AgentContext, { agent, taskId: row.task_id, attempt }));
        if (hasModel) services = Context.add(services, Generating, generating(row));
        const exit = yield* handler({ payload: payload.success, taskId: row.task_id, attempt, agent }).pipe(
          Effect.provideContext(services as Context.Context<unknown>),
          // Agent calls go through the outbox; a waiting one suspends the task.
          Effect.provideService(Calls, calls),
          Effect.provideService(ExternalTask, external),
          Effect.provideService(CurrentExecution, execution),
          Effect.annotateLogs({ agent: `${type}/${agent.name}`, taskId: row.task_id, attempt }),
          Effect.exit,
        );
        if (exit._tag === "Success") {
          const encoded = yield* Schema.encodeUnknownEffect(json(rpc.successSchema))(exit.value).pipe(Effect.orDie);
          return { _tag: "Completed", result: encoded } as Outcome;
        }
        const error = Cause.findErrorOption(exit.cause);
        if (Option.isSome(error)) {
          const e = error.value;
          // A declared error is part of the contract: it settles the task and reaches the caller as itself.
          if (Schema.is(rpc.errorSchema as Schema.Top)(e)) {
            const value = yield* Schema.encodeUnknownEffect(json(rpc.errorSchema))(e).pipe(Effect.orDie);
            return { _tag: "Failed", permanent: true, error: { kind: "declared", value } } as Outcome;
          }
          // A failed LLM turn fails the same way on every rerun, so it settles the task.
          if ((e as object) instanceof GenerationError) {
            return {
              _tag: "Failed",
              permanent: true,
              error: agentFailure(
                new GenerationFailed({ message: `${(e as GenerationError).reason._tag}: ${messageOf((e as GenerationError).reason)}` }),
              ),
            } as Outcome;
          }
          // A workspace denial fails the same way on every rerun (grants don't change on retry).
          if ((e as object) instanceof FsDenied) return { _tag: "Failed", permanent: true, error: agentFailure(e as FsDenied) } as Outcome;
          return { _tag: "Failed", permanent: false, error: { kind: "attempt", message: messageOf(e) } } as Outcome;
        }
        const defect = Cause.findDefect(exit.cause);
        if (defect._tag === "Success" && defect.success instanceof Suspend) return { _tag: "Suspended" } as Outcome;
        return {
          _tag: "Failed",
          permanent: false,
          error: { kind: "attempt", message: defect._tag === "Success" ? messageOf(defect.success) : "the handler was interrupted" },
        } as Outcome;
      });

      // A failed attempt either settles the task or puts it back with a backoff.
      const afterFailure = (row: Inbox.InboxRow, attempts: number, error: Inbox.StoredFailure, permanent: boolean) =>
        permanent
          ? Inbox.fail(sql, row.seq, error, now())
          : attempts >= row.max_attempts
            ? Inbox.fail(
                sql,
                row.seq,
                agentFailure(new RetriesExhausted({ attempts, lastError: error.kind === "attempt" ? error.message : "" })),
                now(),
              )
            : Inbox.retryLater(sql, row.seq, error, now() + backoff(row.task, attempts));

      const drainPass = Effect.gen(function* () {
        // The drain is single-flight per Durable Object, so a `running` row at the start of a
        // pass belongs to an execution that ended without settling it: a lost attempt.
        for (const row of yield* Inbox.running(sql)) {
          yield* transaction(
            Effect.andThen(
              row.attempts >= row.max_attempts
                ? Inbox.fail(sql, row.seq, agentFailure(new ExecutionLost({ attempts: row.attempts })), now())
                : Inbox.retryLater(sql, row.seq, { kind: "attempt", message: "the execution running this task ended" }, now() + backoff(row.task, row.attempts)),
              outbox.notify(row.task_id),
            ),
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
          const { outcome: result, flush } = yield* execute(row, attempt);
          // The outcome, the calls the handler made (unless it failed), and the replies owed to
          // whoever waits for this task commit together.
          yield* transaction(
            Effect.gen(function* () {
              if (result._tag === "Completed") yield* Effect.andThen(Inbox.complete(sql, row.seq, result.result, now()), flush);
              else if (result._tag === "Suspended") yield* Effect.andThen(Inbox.suspend(sql, row.seq), flush);
              else yield* afterFailure(row, attempt, result.error, result.permanent);
              yield* outbox.notify(row.task_id);
            }),
          );
          if (result._tag !== "Suspended") {
            const settled = yield* Inbox.get(sql, row.task_id);
            if (settled !== undefined && Inbox.isSettled(settled)) {
              yield* Effect.logInfo("task.settled").pipe(
                Effect.annotateLogs({ agent: `${type}/${agent.name}`, taskId: row.task_id, status: settled.status, causation: settled.causation }),
              );
            }
          }
          yield* release(row.task_id);
          processed++;
        }
        const next = yield* Inbox.nextRunAt(sql);
        // `progress` must strictly increase, or Alchemy counts the reschedule as a retry.
        if (next !== undefined) yield* scheduleDrain(next, yield* Inbox.advanceCursor(sql));
      });

      drain = yield* Alchemy.makeCallback(DRAIN_CALLBACK, (_: { readonly v: 1 }) => drainPass, { retry: { delay: "1 second" } });
      // Registered on every agent (callback names are a persisted contract), used by LLM turns.
      stepCallback = yield* Alchemy.makeCallback(
        STEP_CALLBACK,
        (p: { readonly v: 1; readonly turnId: string }) => sessions.step(p.turnId),
        { retry: { delay: "1 second" } },
      );
      // M3: delivers this agent's outbox (calls it makes, replies it owes).
      // M7: resumes a task that waits for something outside, at the time it asked for.
      wakeCallback = yield* Alchemy.makeCallback(
        WAKE_CALLBACK,
        (p: { readonly v: 1; readonly taskId: string }) => transaction(resumeTask(p.taskId)),
        { retry: { delay: "1 second" } },
      );
      outboxCallback = yield* Alchemy.makeCallback(OUTBOX_CALLBACK, (_: { readonly v: 1 }) => outbox.deliveryPass, {
        retry: { delay: "1 second" },
      });

      /** Whether this activation has told the registry about this agent. */
      let reported = false;
      const record = (taskId: string) =>
        Inbox.get(sql, taskId).pipe(Effect.map((row) => (row === undefined ? null : Inbox.toRecord(row))));

      const rpc: AgentRpc = {
        enqueue: (taskId, tag, payload, options) =>
          Effect.gen(function* () {
          const { recorded, isNew } = yield* transaction(
            Effect.gen(function* () {
              const def = contract.rpcs.get(tag);
              // Unknown tasks and invalid payloads are recorded as failed, not thrown.
              const failure: Inbox.StoredFailure | undefined =
                def === undefined
                  ? agentFailure(new UnknownTask({ tag }))
                  : yield* Schema.decodeUnknownEffect(json(def.payloadSchema))(payload).pipe(
                      Effect.as(undefined),
                      Effect.catch((issue) => Effect.succeed(agentFailure(new InvalidPayload({ message: issue.message })))),
                    );
              const { row, isNew } = yield* Inbox.insert(sql, {
                taskId,
                task: tag,
                input: payload,
                maxAttempts: policyOf(tag).maxAttempts,
                causation: options.causation,
                now: now(),
                ...(failure ? { failure } : {}),
              });
              // Scheduled in the same transaction as the write (D4).
              if (isNew && !failure) yield* scheduleDrain(now());
              return { recorded: Inbox.toRecord(row), isNew };
            }),
          );
          // M7: the registry learns about this agent once per activation (best effort).
          if (isNew && !reported) {
            reported = true;
            yield* reportSeen(agent, now());
          }
          return recorded;
          }),
        get: record,
        wait: (taskId, timeoutMs) =>
          Effect.gen(function* () {
            const current = yield* record(taskId);
            if (current === null || current.status === "completed" || current.status === "failed") return current;
            const d = yield* Deferred.make<void>();
            waiters.set(taskId, [...(waiters.get(taskId) ?? []), d]);
            yield* Deferred.await(d).pipe(
              Effect.timeoutOption(Duration.millis(timeoutMs)),
              Effect.ensuring(Effect.sync(() => waiters.set(taskId, (waiters.get(taskId) ?? []).filter((x) => x !== d)))),
            );
            return yield* record(taskId);
          }),
        transcript: (sessionId, after) =>
          sessions.messages(sessionId, after).pipe(
            Effect.map((rows) =>
              rows.map((r) => ({ seq: r.seq, turnId: r.turn_id, kind: r.kind, message: JSON.parse(r.message), createdAt: r.created_at })),
            ),
          ),
        events: (sessionId, after) => sessions.events(sessionId, after),
        subscribe: (taskId, replyTo) => outbox.subscribe(taskId, replyTo),
        reply: (id, record) => outbox.reply(id, record),
        resolve: (taskId, outcome) =>
          Effect.gen(function* () {
            const resolved = yield* transaction(
              Effect.gen(function* () {
                const ok = yield* Inbox.resolveWaiting(sql, taskId, outcome, now());
                if (ok) yield* outbox.notify(taskId);
                return ok;
              }),
            );
            if (resolved) {
              yield* release(taskId);
              const row = yield* Inbox.get(sql, taskId);
              yield* Effect.logInfo("task.settled").pipe(
                Effect.annotateLogs({ agent: `${type}/${agent.name}`, taskId, status: row?.status, causation: row?.causation, by: "resolve" }),
              );
            }
            return { resolved, record: yield* record(taskId) };
          }),
        tasks: (query) => Inbox.list(sql, query),
      };
      return rpc;
    });
  });
};
