// Durable LLM turns (D6): a turn is a sequence of steps, each persisted before the next is
// scheduled. A step is one model call (tool resolution disabled) or exactly one tool call.
// Design and evidence: docs/spike/0.5-llm.md, docs/ARCHITECTURE.md ("LLM turns").
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as AiError from "effect/ai/AiError";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Prompt from "effect/ai/Prompt";
import type * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import type * as JsonSchema from "effect/JsonSchema";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SchemaRepresentation from "effect/SchemaRepresentation";
import * as Stream from "effect/Stream";
import { AgentContext, Calls, CurrentExecution, GenerationError, ModelError, StepLimit, Suspend, TokenBudget, ToolCall } from "./core.ts";
import { advanceCursor, type Sql } from "./inbox.ts";

export interface Limits {
  /** Steps per turn (model calls plus tool calls). Default 24. */
  readonly maxSteps?: number;
  /** Input plus output tokens per turn. Default: unlimited. */
  readonly maxTokens?: number;
  /** Time allowed for one model call. Default 60 seconds. */
  readonly stepTimeout?: Duration.Input;
}

/** How a turn's answer is produced and checked. Stored with the turn. */
export type OutputSpec =
  | { readonly mode: "text"; readonly document?: JsonSchema.Document<"draft-2020-12"> }
  | { readonly mode: "respond"; readonly tool: JsonSchema.JsonSchema; readonly document: JsonSchema.Document<"draft-2020-12"> };

type TurnRow = {
  id: string;
  session_id: string;
  task_id: string;
  /** `waiting`: a tool call is waiting for another agent's answer (M3). */
  status: "queued" | "running" | "waiting" | "completed" | "failed";
  message: string;
  system: string | null;
  output_schema: string | null;
  steps: number;
  errors: number;
  ticks: number;
  tokens_in: number;
  tokens_out: number;
  output: string | null;
  error: string | null;
};
type MessageRow = { seq: number; turn_id: string; kind: string; message: string; created_at: number };

export type SessionEvent =
  | { readonly type: "message"; readonly seq: number; readonly turnId: string; readonly kind: string; readonly message: unknown }
  | { readonly type: "turn"; readonly turnId: string; readonly status: string; readonly error?: unknown }
  | { readonly type: "idle" };

export interface TurnOutcome {
  readonly status: "completed" | "failed";
  readonly output: unknown;
  readonly error: GenerationError | undefined;
}

const RESPOND = "respond";
const MAX_MODEL_ERRORS = 3;
const MessageJson = Schema.fromJsonString(Schema.toCodecJson(Prompt.Message));
const encodeMessage = Schema.encodeSync(MessageJson);
const decodeMessage = Schema.decodeUnknownSync(MessageJson);
const ReasonJson = Schema.fromJsonString(Schema.toCodecJson(GenerationError.fields.reason));

/** Validates an answer against the turn's output document; returns the problem, if any. */
const validate = (document: JsonSchema.Document<"draft-2020-12">, value: unknown) =>
  Schema.decodeUnknownEffect(SchemaRepresentation.fromJsonSchemaDocument(document) as Schema.Codec<unknown, unknown, never, never>)(value).pipe(
    Effect.as(undefined),
    Effect.catch((issue) => Effect.succeed(issue.message)),
  );

export interface SessionDeps {
  readonly sql: Sql;
  readonly transaction: <A>(effect: Effect.Effect<A, never, RuntimeContext>) => Effect.Effect<A, never, RuntimeContext>;
  readonly now: () => number;
  /** The agent implementation's context: the LanguageModel and tool handlers come from here. */
  readonly context: Context.Context<never>;
  /** Provides the agent's model to a model call. */
  readonly provideModel: <A, E>(effect: Effect.Effect<A, E, LanguageModel.LanguageModel>) => Effect.Effect<A, E, RuntimeContext>;
  readonly toolkit: Toolkit.Any | undefined;
  readonly limits: Limits | undefined;
  /** AgentContext for the task that started a turn (tools may depend on it). */
  readonly taskServices: (taskId: string) => Effect.Effect<Context.Context<AgentContext>, never, RuntimeContext>;
  readonly execution: Context.Service.Shape<typeof CurrentExecution>;
  /** Schedules the turn's next step (durable callback, job id = turn id). */
  readonly scheduleStep: (turnId: string, afterMs: number, progress: number) => Effect.Effect<void, never, RuntimeContext>;
  /** Called in the transaction that settles a turn: resume the task waiting on it. */
  readonly onSettled: (taskId: string) => Effect.Effect<void, never, RuntimeContext>;
  /**
   * The `Calls` service for one tool call, and `flush`, run in the transaction that records the
   * tool's result (or the turn's suspension).
   */
  readonly callsFor: (
    turn: { readonly id: string; readonly task_id: string },
    toolCallId: string,
  ) => { readonly service: Context.Service.Shape<typeof Calls>; readonly flush: Effect.Effect<void, never, RuntimeContext> };
}

export const makeSessions = (deps: SessionDeps) => {
  const { sql, transaction, now } = deps;
  const maxSteps = deps.limits?.maxSteps ?? 24;
  const maxTokens = deps.limits?.maxTokens;
  const stepTimeout = Duration.fromInputUnsafe(deps.limits?.stepTimeout ?? "60 seconds");
  const userTools = deps.toolkit ? Object.values((deps.toolkit as Toolkit.Toolkit<Record<string, Tool.Any>>).tools) : [];

  const all = <T extends Record<string, any>>(query: string, ...bindings: Array<unknown>) =>
    Effect.flatMap(sql.exec<T>(query, ...bindings), (c) => c.toArray());

  const getTurn = (id: string) => all<TurnRow>("SELECT * FROM alchemy_agent_turns WHERE id = ?", id).pipe(Effect.map((r) => r[0]));

  const messages = (sessionId: string, after = 0) =>
    all<MessageRow>(
      "SELECT seq, turn_id, kind, message, created_at FROM alchemy_agent_transcript WHERE session_id = ? AND seq > ? ORDER BY seq",
      sessionId,
      after,
    );

  // ---- The real output schema of each turn, for full validation (transformations included).
  // In memory only: after an eviction, validation falls back to the stored JSON Schema document.
  const schemas = new Map<string, Schema.Codec<any, any, never, never>>();
  const check = (turnId: string, document: JsonSchema.Document<"draft-2020-12"> | undefined, value: unknown) => {
    const schema = schemas.get(turnId);
    if (schema !== undefined) {
      return Schema.decodeUnknownEffect(schema)(value).pipe(
        Effect.as(undefined),
        Effect.catch((issue) => Effect.succeed(issue.message)),
      );
    }
    return document === undefined ? Effect.succeed(undefined) : validate(document, value);
  };

  // ---- In-memory listeners for the events stream (lost on eviction; clients reconnect) -----
  const listeners = new Map<string, Set<(e: SessionEvent) => void>>();
  const emit = (sessionId: string, e: SessionEvent) => Effect.sync(() => listeners.get(sessionId)?.forEach((f) => f(e)));

  const append = Effect.fnUntraced(function* (turn: TurnRow, kind: "user" | "model" | "tool_result", message: Prompt.Message) {
    const next = yield* sql
      .exec<{ seq: number }>("SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM alchemy_agent_transcript WHERE session_id = ?", turn.session_id)
      .pipe(Effect.flatMap((c) => c.one()));
    const encoded = encodeMessage(message);
    yield* sql.exec(
      "INSERT INTO alchemy_agent_transcript (session_id, seq, turn_id, kind, message, created_at) VALUES (?, ?, ?, ?, ?, ?)",
      turn.session_id,
      next.seq,
      turn.id,
      kind,
      encoded,
      now(),
    );
    return { type: "message", seq: next.seq, turnId: turn.id, kind, message: JSON.parse(encoded) } as const;
  });

  const schedule = Effect.fnUntraced(function* (turn: TurnRow, afterMs = 0) {
    yield* sql.exec("UPDATE alchemy_agent_turns SET ticks = ticks + 1 WHERE id = ?", turn.id);
    // `progress` must strictly increase, or Alchemy counts the reschedule as a retry (0.3/E9). It
    // must be one counter for the whole Durable Object: when a step of one turn schedules
    // another turn's step, Alchemy compares against the scheduling job's progress and silently
    // drops a lower value as stale.
    yield* deps.scheduleStep(turn.id, afterMs, yield* advanceCursor(sql, "step_cursor"));
  });

  /** Starts the oldest queued turn of a session, unless one is already running. */
  const startNext = Effect.fnUntraced(function* (sessionId: string) {
    const running = yield* all<{ id: string }>("SELECT id FROM alchemy_agent_turns WHERE session_id = ? AND status IN ('running', 'waiting') LIMIT 1", sessionId);
    if (running.length > 0) return [] as Array<SessionEvent>;
    const [turn] = yield* all<TurnRow>(
      "SELECT * FROM alchemy_agent_turns WHERE session_id = ? AND status = 'queued' ORDER BY created_at, id LIMIT 1",
      sessionId,
    );
    if (turn === undefined) return [] as Array<SessionEvent>;
    yield* sql.exec("UPDATE alchemy_agent_turns SET status = 'running', started_at = ? WHERE id = ?", now(), turn.id);
    const event = yield* append(turn, "user", Prompt.userMessage({ content: [Prompt.textPart({ text: turn.message })] }));
    yield* schedule(turn);
    return [event, { type: "turn", turnId: turn.id, status: "running" }] as Array<SessionEvent>;
  });

  /**
   * Records a new turn (idempotent: the turn id is derived from the task and the call's position
   * in its handler) and starts it if the session is free. Run inside a transaction.
   */
  const createTurn = Effect.fnUntraced(function* (turn: {
    readonly id: string;
    readonly sessionId: string;
    readonly taskId: string;
    readonly message: string;
    readonly system: string | undefined;
    readonly output: OutputSpec;
    readonly schema: Schema.Codec<any, any, never, never> | undefined;
  }) {
    if (turn.schema !== undefined) schemas.set(turn.id, turn.schema);
    yield* sql.exec(
      `INSERT INTO alchemy_agent_turns (id, session_id, task_id, status, message, system, output_schema, created_at)
       VALUES (?, ?, ?, 'queued', ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING`,
      turn.id,
      turn.sessionId,
      turn.taskId,
      turn.message,
      turn.system ?? null,
      JSON.stringify(turn.output),
      now(),
    );
    return yield* startNext(turn.sessionId);
  });

  const settle = Effect.fnUntraced(function* (
    turn: TurnRow,
    outcome: { readonly output?: unknown; readonly error?: StepLimit | TokenBudget | ModelError },
  ) {
    const events = yield* transaction(
      Effect.gen(function* () {
        const status = outcome.error ? "failed" : "completed";
        const updated = yield* all<{ id: string }>(
          "UPDATE alchemy_agent_turns SET status = ?, output = ?, error = ?, settled_at = ? WHERE id = ? AND status = 'running' RETURNING id",
          status,
          outcome.error ? null : JSON.stringify(outcome.output ?? null),
          outcome.error ? Schema.encodeSync(ReasonJson)(outcome.error) : null,
          now(),
          turn.id,
        );
        // Canceled (with its task) while this step ran: it already settled.
        if (updated.length === 0) return [] as Array<SessionEvent>;
        yield* deps.onSettled(turn.task_id);
        const started = yield* startNext(turn.session_id);
        return [{ type: "turn", turnId: turn.id, status, ...(outcome.error ? { error: outcome.error } : {}) } as SessionEvent, ...started];
      }),
    );
    for (const e of events) yield* emit(turn.session_id, e);
    if (!events.some((e) => e.type === "turn" && e.status === "running")) yield* emit(turn.session_id, { type: "idle" });
  });

  const toolCallsOf = (m: Prompt.Message) =>
    m.role === "assistant" && typeof m.content !== "string" ? m.content.filter((p): p is Prompt.ToolCallPart => p.type === "tool-call") : [];
  const textOf = (m: Prompt.Message) =>
    typeof m.content === "string" ? m.content : m.content.flatMap((p) => (p.type === "text" ? [p.text] : [])).join("");

  /** Persists one step's message, counts the step, and schedules the next. */
  const persistStep = Effect.fnUntraced(function* (
    turn: TurnRow,
    kind: "model" | "tool_result" | "user",
    message: Prompt.Message,
    usage?: { readonly in: number; readonly out: number },
    alongside: Effect.Effect<void, never, RuntimeContext> = Effect.void,
  ) {
    const event = yield* transaction(
      Effect.gen(function* () {
        // The turn may have been canceled (with its task) while this step ran: drop the step, and
        // the calls its tool made.
        const current = yield* getTurn(turn.id);
        if (current?.status !== "running") return undefined;
        yield* alongside;
        const e = yield* append(turn, kind, message);
        yield* sql.exec(
          "UPDATE alchemy_agent_turns SET steps = steps + 1, errors = 0, tokens_in = tokens_in + ?, tokens_out = tokens_out + ? WHERE id = ?",
          usage?.in ?? 0,
          usage?.out ?? 0,
          turn.id,
        );
        yield* schedule(turn);
        return e;
      }),
    );
    if (event !== undefined) yield* emit(turn.session_id, event);
  });

  const toolResult = (call: Prompt.ToolCallPart, isFailure: boolean, result: unknown) =>
    Prompt.toolMessage({ content: [Prompt.toolResultPart({ id: call.id, name: call.name, isFailure, providerExecuted: false, result })] });

  // ---- One step --------------------------------------------------------------------------

  /** Runs one tool call with the toolkit's handlers. Invalid arguments and failures go back to the model. */
  const runTool = Effect.fnUntraced(function* (
    turn: TurnRow,
    call: Prompt.ToolCallPart,
    calls: ReturnType<SessionDeps["callsFor"]>,
  ): Effect.fn.Return<{ readonly isFailure: boolean; readonly result: unknown } | "suspended", never, RuntimeContext> {
    const fail = (message: string) => ({ isFailure: true, result: { error: message } as unknown });
    if (deps.toolkit === undefined || !userTools.some((t) => t.name === call.name)) return fail(`Unknown tool "${call.name}"`);
    const services = Context.add(yield* deps.taskServices(turn.task_id), ToolCall, {
      sessionId: turn.session_id,
      turnId: turn.id,
      toolCallId: call.id,
      idempotencyKey: `${turn.session_id}/${turn.id}/${call.id}`,
    });
    return yield* Effect.gen(function* () {
      const handled = yield* (deps.toolkit as Toolkit.Toolkit<Record<string, Tool.Any>>);
      const results = yield* handled.handle(call.name as never, call.params as never, call.id).pipe(Effect.flatMap(Stream.runCollect));
      const last = [...results].at(-1);
      return last === undefined ? fail("the tool returned nothing") : { isFailure: last.isFailure, result: last.encodedResult };
    }).pipe(
      // Tools that declare `dependencies: [AgentContext, ToolCall]` get them here.
      Effect.provideContext(Context.merge(deps.context, services) as Context.Context<never>),
      Effect.provideService(CurrentExecution, deps.execution),
      // Agent calls in a tool go through the outbox; a waiting one suspends the turn.
      Effect.provideService(Calls, calls.service),
      Effect.catch((e) => Effect.succeed(fail(AiError.isAiError(e) ? e.message : String((e as { message?: unknown })?.message ?? e)))),
      Effect.catchDefect((d) =>
        Effect.succeed(d instanceof Suspend ? ("suspended" as const) : fail(d instanceof Error ? d.message : String(d))),
      ),
    );
  });

  const callModel = (turn: TurnRow, spec: OutputSpec, rows: ReadonlyArray<MessageRow>, forceRespond: boolean) => {
    const respondTool = spec.mode === "respond" ? [Tool.dynamic(RESPOND, { description: "Give your final answer.", parameters: spec.tool })] : [];
    const system = [turn.system ?? "", spec.mode === "respond" ? `When you have the final answer, call the \`${RESPOND}\` tool with it.` : ""]
      .filter(Boolean)
      .join("\n\n");
    const prompt = Prompt.fromMessages([...(system ? [Prompt.systemMessage({ content: system })] : []), ...rows.map((r) => decodeMessage(r.message))]);
    const tools = [...userTools, ...respondTool];
    // Options are assembled at runtime (tools vary per agent and per turn), hence the cast.
    const options = {
      prompt,
      ...(tools.length > 0 ? { toolkit: Toolkit.make(...tools) } : {}),
      disableToolCallResolution: true,
      toolCallValidation: "deferred",
      ...(forceRespond ? { toolChoice: { tool: RESPOND } } : {}),
    };
    return LanguageModel.generateText(options as never as { readonly prompt: Prompt.Prompt }).pipe(Effect.timeout(stepTimeout), deps.provideModel);
  };

  /** Runs exactly one step of a turn, persists it, and schedules the next. Never fails. */
  const step = Effect.fn("Agent.step")(function* (turnId: string) {
    const turn = yield* getTurn(turnId);
    if (turn === undefined || turn.status !== "running") return;
    if (turn.steps >= maxSteps) return yield* settle(turn, { error: new StepLimit({ steps: turn.steps }) });
    if (maxTokens !== undefined && turn.tokens_in + turn.tokens_out > maxTokens) {
      return yield* settle(turn, { error: new TokenBudget({ tokens: turn.tokens_in + turn.tokens_out }) });
    }
    const spec: OutputSpec = turn.output_schema === null ? { mode: "text" } : JSON.parse(turn.output_schema);
    const rows = yield* messages(turn.session_id);
    const mine = rows.filter((r) => r.turn_id === turn.id);
    const lastModel = [...mine].reverse().find((r) => r.kind === "model");
    const lastModelMessage = lastModel ? decodeMessage(lastModel.message) : undefined;
    const calls = lastModelMessage ? toolCallsOf(lastModelMessage) : [];
    const answered = new Set(
      mine
        .filter((r) => r.kind === "tool_result" && lastModel !== undefined && r.seq > lastModel.seq)
        .flatMap((r) => {
          const m = decodeMessage(r.message);
          return typeof m.content === "string" ? [] : m.content.flatMap((p) => ("id" in p ? [p.id] : []));
        }),
    );
    const repliedAfter = mine.some((r) => r.kind === "user" && lastModel !== undefined && r.seq > lastModel.seq);

    // A `respond` call is the structured answer. A valid one settles the turn; an invalid one
    // goes back to the model as a failed tool result, so it can answer again.
    const respondCall = calls.find((c) => c.name === RESPOND && !answered.has(c.id));
    if (respondCall && spec.mode === "respond") {
      const invalid = yield* check(turn.id, spec.document, respondCall.params);
      if (invalid === undefined) return yield* settle(turn, { output: respondCall.params });
      return yield* persistStep(
        turn,
        "tool_result",
        toolResult(respondCall, true, { error: `Invalid answer: ${invalid}. Call ${RESPOND} again with a valid answer.` }),
      );
    }

    // Exactly one tool call per step.
    const pending = calls.filter((c) => !answered.has(c.id) && c.name !== RESPOND);
    if (pending.length > 0) {
      const call = pending[0]!;
      const calls = deps.callsFor(turn, call.id);
      const result = yield* runTool(turn, call, calls);
      if (result === "suspended") {
        // The tool waits for another agent: the turn holds no execution until the answer
        // arrives, then this step runs again and the tool gets the recorded answer.
        const waiting = yield* transaction(
          Effect.gen(function* () {
            const updated = yield* all<{ id: string }>(
              "UPDATE alchemy_agent_turns SET status = 'waiting' WHERE id = ? AND status = 'running' RETURNING id",
              turn.id,
            );
            // Canceled while the tool ran: its calls are never sent.
            if (updated.length === 0) return false;
            yield* calls.flush;
            return true;
          }),
        );
        if (!waiting) return;
        return yield* emit(turn.session_id, { type: "turn", turnId: turn.id, status: "waiting" });
      }
      return yield* persistStep(turn, "tool_result", toolResult(call, result.isFailure, result.result), undefined, calls.flush);
    }

    // A text answer with no tool calls ends a text turn, if it validates.
    if (lastModelMessage !== undefined && calls.length === 0 && !repliedAfter && spec.mode === "text") {
      const text = textOf(lastModelMessage);
      const invalid = yield* check(turn.id, spec.document, text);
      if (invalid === undefined) return yield* settle(turn, { output: text });
      return yield* persistStep(
        turn,
        "user",
        Prompt.userMessage({ content: [Prompt.textPart({ text: `That answer is invalid: ${invalid}. Answer again.` })] }),
      );
    }

    // A model call. If a structured answer is due but the model replied in text (or its last
    // `respond` was invalid), force the `respond` tool.
    const forceRespond =
      spec.mode === "respond" &&
      lastModelMessage !== undefined &&
      (calls.length === 0 || calls.every((c) => c.name === RESPOND && answered.has(c.id)));
    const response = yield* callModel(turn, spec, rows, forceRespond).pipe(Effect.result);
    const ok = response._tag === "Success" && (response.success.text.trim() !== "" || response.success.toolCalls.length > 0);
    if (!ok) {
      // Includes the empty response Alchemy's adapter returns for provider errors (0.5/L10b).
      const message =
        response._tag === "Failure"
          ? String((response.failure as { message?: unknown })?.message ?? response.failure)
          : "the model returned an empty response";
      if (turn.errors + 1 >= MAX_MODEL_ERRORS) return yield* settle(turn, { error: new ModelError({ message }) });
      yield* transaction(
        Effect.gen(function* () {
          yield* sql.exec("UPDATE alchemy_agent_turns SET errors = errors + 1 WHERE id = ?", turn.id);
          yield* schedule(turn, 1000 * 2 ** turn.errors);
        }),
      );
      return;
    }
    const r = response.success;
    const assistant =
      Prompt.fromResponseParts((r.content as ReadonlyArray<Response.AnyPart>).filter((p) => p.type === "text" || p.type === "tool-call"))
        .content[0] ?? Prompt.assistantMessage({ content: [Prompt.textPart({ text: r.text })] });
    yield* persistStep(turn, "model", assistant, { in: r.usage.inputTokens.total ?? 0, out: r.usage.outputTokens.total ?? 0 });
  });

  /** The stored outcome of a settled turn, "pending" while it runs, undefined if unknown. */
  const outcome = (turnId: string) =>
    getTurn(turnId).pipe(
      Effect.map((t): TurnOutcome | "pending" | undefined =>
        t === undefined
          ? undefined
          : t.status === "completed" || t.status === "failed"
            ? {
                status: t.status,
                output: t.output === null ? undefined : JSON.parse(t.output),
                error: t.error === null ? undefined : new GenerationError({ reason: Schema.decodeUnknownSync(ReasonJson)(t.error) }),
              }
            : "pending",
      ),
    );

  /** The session's transcript after `after`, then new messages until the session is idle. */
  const events = (sessionId: string, after: number) =>
    Stream.callback<SessionEvent, never, RuntimeContext>((queue) =>
      Effect.gen(function* () {
        const set = listeners.get(sessionId) ?? new Set();
        listeners.set(sessionId, set);
        let done = false;
        const offer = (e: SessionEvent) => {
          if (done) return;
          Queue.offerUnsafe(queue, e);
          if (e.type === "idle") {
            done = true;
            Queue.endUnsafe(queue);
          }
        };
        set.add(offer);
        yield* Effect.addFinalizer(() => Effect.sync(() => set.delete(offer)));
        for (const r of yield* messages(sessionId, after)) {
          offer({ type: "message", seq: r.seq, turnId: r.turn_id, kind: r.kind, message: JSON.parse(r.message) });
        }
        const active = yield* all<{ n: number }>(
          "SELECT count(*) AS n FROM alchemy_agent_turns WHERE session_id = ? AND status IN ('queued', 'running', 'waiting')",
          sessionId,
        );
        if ((active[0]?.n ?? 0) === 0) offer({ type: "idle" });
      }),
    );

  /** In the transaction that records an answer a turn waited for: run its next step. */
  const resume = Effect.fnUntraced(function* (turnId: string) {
    const [turn] = yield* all<TurnRow>("UPDATE alchemy_agent_turns SET status = 'running' WHERE id = ? AND status = 'waiting' RETURNING *", turnId);
    if (turn !== undefined) yield* schedule(turn);
  });

  /**
   * In the transaction that cancels a task: fails its unsettled turns, so no further step runs
   * (a step already running drops its result), and starts the next turn of each session they
   * held. Returns the events to emit after the transaction commits.
   */
  const cancelTask = Effect.fnUntraced(function* (taskId: string, reason: string) {
    const canceled = yield* all<{ id: string; session_id: string }>(
      `UPDATE alchemy_agent_turns SET status = 'failed', error = ?, settled_at = ?
       WHERE task_id = ? AND status IN ('queued', 'running', 'waiting') RETURNING id, session_id`,
      Schema.encodeSync(ReasonJson)(new ModelError({ message: `the task was canceled: ${reason}` })),
      now(),
      taskId,
    );
    const events: Array<{ readonly sessionId: string; readonly event: SessionEvent }> = [];
    for (const t of canceled) events.push({ sessionId: t.session_id, event: { type: "turn", turnId: t.id, status: "failed" } });
    for (const sessionId of new Set(canceled.map((t) => t.session_id))) {
      const started = yield* startNext(sessionId);
      for (const event of started) events.push({ sessionId, event });
      if (started.length === 0) events.push({ sessionId, event: { type: "idle" } });
    }
    return events;
  });

  /**
   * Retention: removes a pruned task's turns, and the transcripts of the sessions they had to
   * themselves (a turn without `session` is its own session). A named session is a conversation
   * other tasks continue, so its transcript stays.
   */
  const removeTask = Effect.fnUntraced(function* (taskId: string) {
    const turns = yield* all<{ id: string; session_id: string }>(
      "DELETE FROM alchemy_agent_turns WHERE task_id = ? AND status IN ('completed', 'failed') RETURNING id, session_id",
      taskId,
    );
    for (const t of turns) {
      if (t.session_id === t.id) yield* sql.exec("DELETE FROM alchemy_agent_transcript WHERE session_id = ?", t.session_id);
    }
  });

  return { createTurn, step, outcome, messages, events, emit, resume, cancelTask, removeTask };
};
