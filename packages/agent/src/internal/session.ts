// LLM sessions and turns (D6): a turn is a sequence of steps, each persisted before the next is
// scheduled. A step is one model call (tool resolution disabled) or exactly one tool call.
// Design and evidence: docs/spike/0.5-llm.md.
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
import type * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SchemaRepresentation from "effect/SchemaRepresentation";
import * as Queue from "effect/Queue";
import type * as JsonSchema from "effect/JsonSchema";
import * as Stream from "effect/Stream";
import { CurrentExecution, PromptFailed, ToolCall } from "./core.ts";
import { advanceCursor, type Sql } from "./inbox.ts";

export interface Limits {
  /** Steps per turn (model calls plus tool calls). Default 24. */
  readonly maxSteps?: number;
  /** Input plus output tokens per turn. Default: unlimited. */
  readonly maxTokens?: number;
  /** Time allowed for one model call. Default 60 seconds. */
  readonly stepTimeout?: Duration.Input;
}

/** Tools for an LLM agent: an Effect AI toolkit plus its handlers (see `Agent.tools`). */
export interface ToolsDef {
  readonly toolkit: Toolkit.Any;
  readonly handlers: Record<string, (params: any) => Effect.Effect<any, any, any>>;
}

type TurnRow = {
  id: string;
  session_id: string;
  task_id: string;
  status: "queued" | "running" | "completed" | "failed";
  message: string;
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
  readonly error: { readonly reason: PromptFailed["reason"]; readonly message: string } | undefined;
}

const RESPOND = "respond";

/** What a turn stores about its structured output. */
interface StoredOutputSchema {
  readonly tool: JsonSchema.JsonSchema;
  readonly document: JsonSchema.Document<"draft-2020-12">;
}

/** Validates a `respond` call against the turn's output schema. */
const validateOutput = (stored: string, value: unknown) => {
  const schema = SchemaRepresentation.fromJsonSchemaDocument((JSON.parse(stored) as StoredOutputSchema).document);
  return Schema.decodeUnknownEffect(schema as Schema.Codec<unknown, unknown, never, never>)(value).pipe(
    Effect.as(undefined),
    Effect.catch((issue) => Effect.succeed(issue.message)),
  );
};
const MAX_MODEL_ERRORS = 3;
const encodeMessage = Schema.encodeSync(Prompt.Message);
const decodeMessage = Schema.decodeUnknownSync(Prompt.Message);

export interface SessionDeps {
  readonly sql: Sql;
  readonly transaction: <A>(effect: Effect.Effect<A, never, RuntimeContext>) => Effect.Effect<A, never, RuntimeContext>;
  readonly now: () => number;
  readonly model: Layer.Layer<LanguageModel.LanguageModel, never, RuntimeContext> | undefined;
  readonly instructions: string | undefined;
  readonly tools: ToolsDef | undefined;
  readonly limits: Limits | undefined;
  /** Services for tool handlers of a turn started by `taskId` (AgentContext, Reach, ...). */
  readonly toolServices: (taskId: string) => Effect.Effect<Context.Context<never>, never, RuntimeContext>;
  readonly execution: Context.Service.Shape<typeof CurrentExecution>;
  /** Schedules the turn's next step (durable callback, id = turn id). */
  readonly scheduleStep: (turnId: string, afterMs: number, progress: number) => Effect.Effect<void, never, RuntimeContext>;
  /** Called in the transaction that settles a turn: resume the task waiting on it. */
  readonly onSettled: (taskId: string) => Effect.Effect<void, never, RuntimeContext>;
}

export const makeSessions = (deps: SessionDeps) => {
  const { sql, transaction, now } = deps;
  const maxSteps = deps.limits?.maxSteps ?? 24;
  const maxTokens = deps.limits?.maxTokens;
  const stepTimeout = Duration.fromInputUnsafe(deps.limits?.stepTimeout ?? "60 seconds");

  const all = <T extends Record<string, any>>(query: string, ...bindings: Array<unknown>) =>
    Effect.flatMap(sql.exec<T>(query, ...bindings), (c) => c.toArray());

  const getTurn = (id: string) =>
    all<TurnRow>("SELECT * FROM alchemy_agent_turns WHERE id = ?", id).pipe(Effect.map((r) => r[0]));

  const messages = (sessionId: string, after = 0) =>
    all<MessageRow>(
      "SELECT seq, turn_id, kind, message, created_at FROM alchemy_agent_transcript WHERE session_id = ? AND seq > ? ORDER BY seq",
      sessionId,
      after,
    );

  // ---- In-memory listeners for the events stream (lost on eviction; clients reconnect) -----
  const listeners = new Map<string, Set<(e: SessionEvent) => void>>();
  const emit = (sessionId: string, e: SessionEvent) =>
    Effect.sync(() => listeners.get(sessionId)?.forEach((f) => f(e)));

  const append = (turn: TurnRow, kind: "user" | "model" | "tool_result", message: Prompt.Message) =>
    Effect.gen(function* () {
      const next = yield* sql
        .exec<{ seq: number }>(
          "SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM alchemy_agent_transcript WHERE session_id = ?",
          turn.session_id,
        )
        .pipe(Effect.flatMap((c) => c.one()));
      const encoded = encodeMessage(message);
      yield* sql.exec(
        "INSERT INTO alchemy_agent_transcript (session_id, seq, turn_id, kind, message, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        turn.session_id,
        next.seq,
        turn.id,
        kind,
        JSON.stringify(encoded),
        now(),
      );
      return { type: "message", seq: next.seq, turnId: turn.id, kind, message: encoded } as const;
    });

  const schedule = (turn: TurnRow, afterMs = 0) =>
    Effect.gen(function* () {
      yield* sql.exec("UPDATE alchemy_agent_turns SET ticks = ticks + 1 WHERE id = ?", turn.id);
      // `progress` must strictly increase, or Alchemy counts the reschedule as a retry (0.3/E9).
      // It must be one counter for the whole Durable Object: when a step of one turn schedules
      // another turn's step, Alchemy compares against the scheduling job's progress and silently
      // drops a lower value as stale.
      yield* deps.scheduleStep(turn.id, afterMs, yield* advanceCursor(sql, "step_cursor"));
    });

  /** Starts the oldest queued turn of a session, unless one is already running. */
  const startNext = (sessionId: string) =>
    Effect.gen(function* () {
      const running = yield* all<{ id: string }>(
        "SELECT id FROM alchemy_agent_turns WHERE session_id = ? AND status = 'running' LIMIT 1",
        sessionId,
      );
      if (running.length > 0) return [];
      const [turn] = yield* all<TurnRow>(
        "SELECT * FROM alchemy_agent_turns WHERE session_id = ? AND status = 'queued' ORDER BY created_at, id LIMIT 1",
        sessionId,
      );
      if (turn === undefined) return [];
      yield* sql.exec("UPDATE alchemy_agent_turns SET status = 'running', started_at = ? WHERE id = ?", now(), turn.id);
      const event = yield* append(turn, "user", Prompt.userMessage({ content: [Prompt.textPart({ text: turn.message })] }));
      yield* schedule(turn);
      return [event, { type: "turn", turnId: turn.id, status: "running" } as const];
    });

  /**
   * Records a new turn (idempotent: the turn id is derived from the task and the prompt's
   * position in its handler) and starts it if the session is free. Run inside a transaction.
   */
  const createTurn = (turn: {
    readonly id: string;
    readonly sessionId: string;
    readonly taskId: string;
    readonly message: string;
    readonly outputSchema: unknown;
  }) =>
    Effect.gen(function* () {
      yield* sql.exec(
        `INSERT INTO alchemy_agent_turns (id, session_id, task_id, status, message, output_schema, created_at)
         VALUES (?, ?, ?, 'queued', ?, ?, ?) ON CONFLICT (id) DO NOTHING`,
        turn.id,
        turn.sessionId,
        turn.taskId,
        turn.message,
        turn.outputSchema === undefined ? null : JSON.stringify(turn.outputSchema),
        now(),
      );
      return yield* startNext(turn.sessionId);
    });

  const settle = (turn: TurnRow, outcome: { output?: unknown; error?: { reason: PromptFailed["reason"]; message: string } }) =>
    Effect.gen(function* () {
      const events = yield* transaction(
        Effect.gen(function* () {
          const status = outcome.error ? "failed" : "completed";
          yield* sql.exec(
            "UPDATE alchemy_agent_turns SET status = ?, output = ?, error = ?, settled_at = ? WHERE id = ? AND status = 'running'",
            status,
            outcome.error ? null : JSON.stringify(outcome.output ?? null),
            outcome.error ? JSON.stringify(outcome.error) : null,
            now(),
            turn.id,
          );
          yield* deps.onSettled(turn.task_id);
          const started = yield* startNext(turn.session_id);
          return [{ type: "turn", turnId: turn.id, status, ...(outcome.error ? { error: outcome.error } : {}) } as SessionEvent, ...started];
        }),
      );
      for (const e of events) yield* emit(turn.session_id, e);
      if (!events.some((e) => e.type === "turn" && e.status === "running")) yield* emit(turn.session_id, { type: "idle" });
    });

  const toolCallsOf = (m: Prompt.Message) =>
    m.role === "assistant" && typeof m.content !== "string"
      ? m.content.filter((p): p is Prompt.ToolCallPart => p.type === "tool-call")
      : [];
  const textOf = (m: Prompt.Message) =>
    typeof m.content === "string"
      ? m.content
      : m.content.flatMap((p) => (p.type === "text" ? [p.text] : [])).join("");

  // ---- One step --------------------------------------------------------------------------

  const runTool = (turn: TurnRow, call: Prompt.ToolCallPart) =>
    Effect.gen(function* () {
      const tools = deps.tools;
      const fail = (message: string) => ({ isFailure: true, result: { error: message } });
      if (tools === undefined || !(call.name in tools.handlers)) return fail(`Unknown tool "${call.name}"`);
      const services = Context.add(yield* deps.toolServices(turn.task_id), ToolCall, {
        sessionId: turn.session_id,
        turnId: turn.id,
        toolCallId: call.id,
        idempotencyKey: `${turn.session_id}/${turn.id}/${call.id}`,
      });
      // Handlers run with the agent's services; Effect AI tool handlers can't require any.
      const handlers = Object.fromEntries(
        Object.entries(tools.handlers).map(([name, h]) => [
          name,
          (params: unknown) =>
            h(params).pipe(
              Effect.provideContext(services as Context.Context<any>),
              Effect.provideService(CurrentExecution, deps.execution),
            ),
        ]),
      );
      const toolkit = tools.toolkit as Toolkit.Toolkit<Record<string, Tool.Any>>;
      return yield* Effect.gen(function* () {
        const handled = yield* toolkit;
        const results = yield* handled.handle(call.name as never, call.params as never, call.id).pipe(Effect.flatMap(Stream.runCollect));
        const last = [...results].at(-1);
        return last === undefined
          ? fail("the tool returned nothing")
          : { isFailure: last.isFailure, result: last.encodedResult };
      }).pipe(
        Effect.provide(toolkit.toLayer(handlers as never) as Layer.Layer<never>),
        // Invalid arguments and typed tool failures go back to the model, so it can correct itself.
        Effect.catch((e) => Effect.succeed(fail(AiError.isAiError(e) ? e.message : String((e as { message?: unknown })?.message ?? e)))),
        Effect.catchDefect((d) => Effect.succeed(fail(d instanceof Error ? d.message : String(d)))),
      );
    });

  const callModel = (turn: TurnRow, rows: ReadonlyArray<MessageRow>, forceRespond: boolean) =>
    Effect.gen(function* () {
      const userTools = deps.tools ? Object.values((deps.tools.toolkit as Toolkit.Toolkit<Record<string, Tool.Any>>).tools) : [];
      const respondTool =
        turn.output_schema === null
          ? []
          : [
              Tool.dynamic(RESPOND, {
                description: "Give your final answer.",
                parameters: (JSON.parse(turn.output_schema) as StoredOutputSchema).tool,
              }),
            ];
      const system = [
        deps.instructions ?? "",
        turn.output_schema === null ? "" : `When you have the final answer, call the \`${RESPOND}\` tool with it.`,
      ]
        .filter(Boolean)
        .join("\n\n");
      const prompt = Prompt.fromMessages([
        ...(system ? [Prompt.systemMessage({ content: system })] : []),
        ...rows.map((r) => decodeMessage(JSON.parse(r.message))),
      ]);
      const tools = [...userTools, ...respondTool];
      // Options are assembled at runtime (tools vary per agent and per turn), hence the cast.
      const options = {
        prompt,
        ...(tools.length > 0 ? { toolkit: Toolkit.make(...tools) } : {}),
        disableToolCallResolution: true,
        toolCallValidation: "deferred",
        ...(forceRespond ? { toolChoice: { tool: RESPOND } } : {}),
      };
      return yield* LanguageModel.generateText(options as never as { readonly prompt: Prompt.Prompt }).pipe(
        Effect.timeout(stepTimeout),
        Effect.provide(deps.model!),
      );
    });

  /** Runs exactly one step of a turn, persists it, and schedules the next. Never fails. */
  const step = (turnId: string) =>
    Effect.gen(function* () {
      const turn = yield* getTurn(turnId);
      if (turn === undefined || turn.status !== "running") return;
      if (turn.steps >= maxSteps) return yield* settle(turn, { error: { reason: "StepLimit", message: `more than ${maxSteps} steps` } });
      if (maxTokens !== undefined && turn.tokens_in + turn.tokens_out > maxTokens) {
        return yield* settle(turn, { error: { reason: "TokenBudget", message: `more than ${maxTokens} tokens` } });
      }
      const rows = yield* messages(turn.session_id);
      const mine = rows.filter((r) => r.turn_id === turn.id);
      const lastModel = [...mine].reverse().find((r) => r.kind === "model");
      const lastModelMessage = lastModel ? decodeMessage(JSON.parse(lastModel.message)) : undefined;
      const calls = lastModelMessage ? toolCallsOf(lastModelMessage) : [];
      const answered = new Set(
        mine
          .filter((r) => r.kind === "tool_result" && lastModel !== undefined && r.seq > lastModel.seq)
          .flatMap((r) => {
            const m = decodeMessage(JSON.parse(r.message));
            return typeof m.content === "string" ? [] : m.content.flatMap((p) => ("id" in p ? [p.id] : []));
          }),
      );

      // A valid `respond` call is the final answer of a turn with an output schema. An invalid one
      // gets a failed tool result, so the model sees the error and answers again.
      const respondCall = calls.find((c) => c.name === RESPOND && !answered.has(c.id));
      if (respondCall && turn.output_schema !== null) {
        const invalid = yield* validateOutput(turn.output_schema, respondCall.params);
        if (invalid === undefined) return yield* settle(turn, { output: respondCall.params });
        const event = yield* transaction(
          Effect.gen(function* () {
            const e = yield* append(
              turn,
              "tool_result",
              Prompt.toolMessage({
                content: [
                  Prompt.toolResultPart({
                    id: respondCall.id,
                    name: RESPOND,
                    isFailure: true,
                    providerExecuted: false,
                    result: { error: `Invalid answer: ${invalid}. Call ${RESPOND} again with a valid answer.` },
                  }),
                ],
              }),
            );
            yield* sql.exec("UPDATE alchemy_agent_turns SET steps = steps + 1 WHERE id = ?", turn.id);
            yield* schedule(turn);
            return e;
          }),
        );
        return yield* emit(turn.session_id, event);
      }

      const pending = calls.filter((c) => !answered.has(c.id) && c.name !== RESPOND);
      if (pending.length > 0) {
        // Exactly one tool call per step.
        const call = pending[0]!;
        const result = yield* runTool(turn, call);
        const event = yield* transaction(
          Effect.gen(function* () {
            const e = yield* append(
              turn,
              "tool_result",
              Prompt.toolMessage({
                content: [
                  Prompt.toolResultPart({
                    id: call.id,
                    name: call.name,
                    isFailure: result.isFailure,
                    providerExecuted: false,
                    result: result.result,
                  }),
                ],
              }),
            );
            yield* sql.exec("UPDATE alchemy_agent_turns SET steps = steps + 1 WHERE id = ?", turn.id);
            yield* schedule(turn);
            return e;
          }),
        );
        return yield* emit(turn.session_id, event);
      }

      if (lastModelMessage !== undefined && calls.length === 0 && turn.output_schema === null) {
        // A plain-text answer with no tool calls ends the turn.
        return yield* settle(turn, { output: textOf(lastModelMessage) });
      }

      // A model call. If a structured answer is due but the model replied in text, force `respond`.
      const forceRespond =
        turn.output_schema !== null &&
        lastModelMessage !== undefined &&
        (calls.length === 0 || calls.every((c) => c.name === RESPOND && answered.has(c.id)));
      const response = yield* callModel(turn, rows, forceRespond).pipe(Effect.result);
      const ok =
        response._tag === "Success" &&
        (response.success.text.trim() !== "" || response.success.toolCalls.length > 0);
      if (!ok) {
        // Includes the empty response Alchemy's adapter returns for provider errors (0.5/L10b).
        const message =
          response._tag === "Failure" ? String((response.failure as { message?: unknown })?.message ?? response.failure) : "the model returned an empty response";
        if (turn.errors + 1 >= MAX_MODEL_ERRORS) {
          return yield* settle(turn, { error: { reason: "ModelError", message } });
        }
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
        Prompt.fromResponseParts(
          (r.content as ReadonlyArray<Response.AnyPart>).filter((p) => p.type === "text" || p.type === "tool-call"),
        ).content[0] ??
        Prompt.assistantMessage({ content: [Prompt.textPart({ text: r.text })] });
      const event = yield* transaction(
        Effect.gen(function* () {
          const e = yield* append(turn, "model", assistant);
          yield* sql.exec(
            "UPDATE alchemy_agent_turns SET steps = steps + 1, errors = 0, tokens_in = tokens_in + ?, tokens_out = tokens_out + ? WHERE id = ?",
            r.usage.inputTokens.total ?? 0,
            r.usage.outputTokens.total ?? 0,
            turn.id,
          );
          yield* schedule(turn);
          return e;
        }),
      );
      yield* emit(turn.session_id, event);
    });

  /** The stored outcome of a settled turn, or undefined while it runs. */
  const outcome = (turnId: string) =>
    getTurn(turnId).pipe(
      Effect.map((t): TurnOutcome | "pending" | undefined =>
        t === undefined
          ? undefined
          : t.status === "completed" || t.status === "failed"
            ? {
                status: t.status,
                output: t.output === null ? undefined : JSON.parse(t.output),
                error: t.error === null ? undefined : JSON.parse(t.error),
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
          "SELECT count(*) AS n FROM alchemy_agent_turns WHERE session_id = ? AND status IN ('queued', 'running')",
          sessionId,
        );
        if ((active[0]?.n ?? 0) === 0) offer({ type: "idle" });
      }),
    );

  return { createTurn, step, outcome, messages, events, emit, getTurn };
};

export type Sessions = ReturnType<typeof makeSessions>;
