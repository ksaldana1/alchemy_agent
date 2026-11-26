// Spike 0.5: an LLM inside a Durable Object, one checkpointed step at a time.
// Throwaway: findings go to docs/spike/0.5-llm.md; the design carries into M2.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as LanguageModel from "effect/ai/LanguageModel";
import * as Prompt from "effect/ai/Prompt";
import type * as Response from "effect/ai/Response";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

export const Gateway = Cloudflare.AI.Gateway("SpikesGateway", { cacheTtl: 0, collectLogs: true });

const DEFAULT_MODEL = "@cf/meta/llama-3.3-70b-instruct-fp8-fast";
const SYSTEM = "You are a support agent. Use the tools to answer. Be brief.";

// --- Tools ------------------------------------------------------------------------------------

const LookupOrder = Tool.make("lookup_order", {
  description: "Look up an order by its id. Returns its status and items.",
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ orderId: Schema.String, status: Schema.String, items: Schema.Array(Schema.String) }),
});
const CurrentTime = Tool.make("current_time", {
  description: "The current time, as an ISO 8601 string.",
  success: Schema.String,
});
export const Tools = Toolkit.make(LookupOrder, CurrentTime);

// --- A scripted model (L8) --------------------------------------------------------------------

/**
 * Answers from a script, indexed by how many assistant messages the prompt already holds, so it
 * is deterministic and replay-safe: the same prompt always gets the same answer.
 */
export const scripted = (script: ReadonlyArray<ReadonlyArray<Response.PartEncoded>>) =>
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: (options) =>
        Effect.sync(() => {
          const turn = options.prompt.content.filter((m) => m.role === "assistant").length;
          return (script[turn] ?? [{ type: "text", text: "(script exhausted)" }]) as never;
        }),
      streamText: () => Stream.empty,
    }),
  );

const ORDER_SCRIPT: ReadonlyArray<ReadonlyArray<Response.PartEncoded>> = [
  [
    { type: "tool-call", id: "call_1", name: "lookup_order", params: { orderId: "42" }, providerExecuted: false },
    { type: "finish", reason: "tool-calls", usage: { inputTokens: { total: 1 }, outputTokens: { total: 1 } } },
  ] as never,
  [
    { type: "text", text: "Order 42 has shipped." },
    { type: "finish", reason: "stop", usage: { inputTokens: { total: 1 }, outputTokens: { total: 1 } } },
  ] as never,
];
const BAD_ARGS_SCRIPT: ReadonlyArray<ReadonlyArray<Response.PartEncoded>> = [
  [
    { type: "tool-call", id: "call_bad", name: "lookup_order", params: { orderId: 42 }, providerExecuted: false },
    { type: "finish", reason: "tool-calls", usage: { inputTokens: { total: 1 }, outputTokens: { total: 1 } } },
  ] as never,
];

// --- The Durable Object -----------------------------------------------------------------------

export type Provider = "workers-ai" | "gateway" | "scripted";
type Row = { seq: number; kind: string; json: string };

const encodeMessage = Schema.encodeSync(Prompt.Message);
const decodeMessage = Schema.decodeUnknownSync(Prompt.Message);

export default class LlmProbe extends Cloudflare.DurableObject<LlmProbe>()(
  "LlmProbe",
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const ai = yield* Cloudflare.Workers.AI();
    const gateway = yield* Cloudflare.AI.QueryGateway(Gateway);

    return Effect.gen(function* () {
      const sql = state.storage.sql;
      yield* sql.exec(`CREATE TABLE IF NOT EXISTS transcript (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT, json TEXT)`);
      yield* sql.exec(`CREATE TABLE IF NOT EXISTS tool_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER, tool_call_id TEXT, name TEXT, params TEXT)`);
      yield* sql.exec(`CREATE TABLE IF NOT EXISTS session (id INTEGER PRIMARY KEY CHECK (id = 1), provider TEXT, model TEXT, chaos INTEGER, chaos_done INTEGER, steps INTEGER, done INTEGER, step_ms TEXT, delay_ms INTEGER)`);

      let currentToolCallId = "";
      const handlers = Tools.toLayer({
        lookup_order: ({ orderId }) =>
          sql
            .exec(
              "INSERT INTO tool_log (at, tool_call_id, name, params) VALUES (?, ?, 'lookup_order', ?)",
              Date.now(),
              currentToolCallId,
              JSON.stringify({ orderId }),
            )
            .pipe(Effect.as({ orderId, status: "shipped", items: ["blue mug", "tea sampler"] })) as Effect.Effect<{
            orderId: string;
            status: string;
            items: Array<string>;
          }>, // storage needs RuntimeContext, always present inside a step (M2: declare tool `dependencies`)
        current_time: () => Effect.sync(() => new Date().toISOString()),
      });

      const modelLayer = (provider: Provider, model: string, script = ORDER_SCRIPT) =>
        provider === "gateway"
          ? gateway.model({ model, parameters: { temperature: 0 } })
          : provider === "scripted"
            ? scripted(script)
            : ai.model({ model, parameters: { temperature: 0 } });

      const rows = () =>
        sql.exec<Row>("SELECT seq, kind, json FROM transcript ORDER BY seq").pipe(Effect.flatMap((c) => c.toArray()));
      const append = (kind: "user" | "model" | "tool_result", message: Prompt.Message) =>
        sql.exec("INSERT INTO transcript (kind, json) VALUES (?, ?)", kind, JSON.stringify(encodeMessage(message)));
      const promptOf = (rs: ReadonlyArray<Row>) =>
        Prompt.fromMessages([Prompt.systemMessage({ content: SYSTEM }), ...rs.map((r) => decodeMessage(JSON.parse(r.json)))]);

      // ---- One step of a turn (L2, L4, L5) ----------------------------------------------------

      type Session = { provider: Provider; model: string; chaos: number; chaos_done: number; steps: number; done: number; delay_ms: number };
      const session = () =>
        sql.exec<Session>("SELECT * FROM session WHERE id = 1").pipe(
          Effect.flatMap((c) => c.toArray()),
          Effect.map((r) => r[0]),
        );

      let stepCallback: Alchemy.Callback<{ readonly v: 1 }>;
      const scheduleStep = (progress: number, delayMs = 0) =>
        stepCallback.schedule("turn", { after: `${delayMs} millis`, payload: { v: 1 }, progress }).pipe(Effect.orDie);

      /** Tool calls in the last model message that have no result row after it. */
      const pendingToolCalls = (rs: ReadonlyArray<Row>) => {
        let lastModel = -1;
        rs.forEach((r, i) => (r.kind === "model" ? (lastModel = i) : undefined));
        if (lastModel < 0) return [];
        const calls = (decodeMessage(JSON.parse(rs[lastModel]!.json)) as Prompt.AssistantMessage).content.filter(
          (p): p is Prompt.ToolCallPart => p.type === "tool-call",
        );
        const answered = new Set(
          rs.slice(lastModel + 1).flatMap((r) =>
            (decodeMessage(JSON.parse(r.json)) as Prompt.ToolMessage).content.flatMap((p) => ("id" in p ? [p.id] : [])),
          ),
        );
        return calls.filter((c) => !answered.has(c.id));
      };

      const step = Effect.gen(function* () {
        const s = yield* session();
        if (s === undefined || s.done === 1) return;
        const started = Date.now();
        const rs = yield* rows();
        const last = rs.at(-1);
        const pending = pendingToolCalls(rs);
        let kind: string;

        if (last?.kind === "model" && pending.length === 0) {
          // The last model message has no tool calls left to run: the turn is complete.
          yield* sql.exec("UPDATE session SET done = 1 WHERE id = 1");
          return;
        } else if (pending.length > 0) {
          // Run exactly ONE tool call, then persist its result.
          const call = pending[0]!;
          currentToolCallId = call.id;
          const tk = yield* Effect.provide(Tools, handlers);
          const results = yield* tk.handle(call.name as "lookup_order", call.params as never, call.id).pipe(
            Effect.flatMap(Stream.runCollect),
          );
          const final = [...results].at(-1)!;
          yield* append(
            "tool_result",
            Prompt.toolMessage({
              content: [
                Prompt.toolResultPart({
                  id: call.id,
                  name: call.name,
                  isFailure: final.isFailure,
                  providerExecuted: false,
                  result: final.encodedResult,
                }),
              ],
            }),
          );
          kind = `tool:${call.name}`;
          // L4 chaos: fail once AFTER the tool result is stored. The retry must not rerun the tool.
          if (s.chaos === 1 && s.chaos_done === 0) {
            yield* sql.exec("UPDATE session SET chaos_done = 1 WHERE id = 1");
            return yield* Effect.die(new Error("chaos: failing after the tool result was stored"));
          }
        } else {
          // Call the model WITHOUT resolving tools (L2): we run them ourselves, one per step.
          const response = yield* LanguageModel.generateText({
            prompt: promptOf(rs),
            toolkit: Tools,
            disableToolCallResolution: true,
          }).pipe(Effect.provide(modelLayer(s.provider, s.model)), Effect.orDie);
          const assistant = Prompt.fromResponseParts(
            response.content.filter((p) => p.type === "text" || p.type === "tool-call"),
          ).content[0] ?? Prompt.assistantMessage({ content: [Prompt.textPart({ text: "" })] });
          yield* append("model", assistant);
          kind = `model:${response.toolCalls.length} tool calls`;
        }
        yield* sql.exec(
          "UPDATE session SET steps = steps + 1, step_ms = json_insert(step_ms, '$[#]', ?) WHERE id = 1",
          `${kind} ${Date.now() - started}ms`,
        );
        if (s.steps + 1 >= 12) return; // step cap
        yield* scheduleStep((yield* rows()).length * 100 + s.steps + 1, s.delay_ms);
      });

      stepCallback = yield* Alchemy.makeCallback("llm-step", (_: { readonly v: 1 }) => step, {
        retry: { delay: "1 second" },
      });

      return {
        // L4/L5/L8: start a turn that runs one step per durable callback.
        start: (text: string, provider: Provider, model: string, chaos: boolean, delayMs = 0) =>
          state.storage.transaction(
            Effect.gen(function* () {
              yield* sql.exec(
                "INSERT INTO session (id, provider, model, chaos, chaos_done, steps, done, step_ms, delay_ms) VALUES (1, ?, ?, ?, 0, 0, 0, '[]', ?) ON CONFLICT (id) DO NOTHING",
                provider,
                model || DEFAULT_MODEL,
                chaos ? 1 : 0,
                delayMs,
              );
              yield* append("user", Prompt.userMessage({ content: [Prompt.textPart({ text })] }));
              yield* scheduleStep(1);
              return { ok: true as const };
            }),
          ),
        status: () =>
          Effect.gen(function* () {
            const s = yield* session();
            const rs = yield* rows();
            const tools = yield* sql.exec<{ n: number }>("SELECT count(*) AS n FROM tool_log").pipe(Effect.flatMap((c) => c.one()));
            return {
              session: s ? { ...s, step_ms: JSON.parse((s as unknown as { step_ms: string }).step_ms) } : null,
              transcript: rs.map((r) => ({ kind: r.kind, message: JSON.parse(r.json) })),
              toolExecutions: tools.n,
            };
          }),

        // L1/L9: a whole turn in one call, letting generateText resolve tools (one round per call).
        prompt: (text: string, provider: Provider, model: string) =>
          Effect.gen(function* () {
            const rounds: Array<unknown> = [];
            let prompt = Prompt.fromMessages([
              Prompt.systemMessage({ content: SYSTEM }),
              Prompt.userMessage({ content: [Prompt.textPart({ text })] }),
            ]);
            for (let i = 0; i < 4; i++) {
              const t0 = Date.now();
              const response = yield* LanguageModel.generateText({ prompt, toolkit: Tools }).pipe(
                Effect.provide(Layer.merge(modelLayer(provider, model || DEFAULT_MODEL), handlers)),
              );
              const finish = response.content.find((p) => p.type === "finish");
              rounds.push({
                ms: Date.now() - t0,
                toolCalls: response.toolCalls.map((c) => ({ name: c.name, params: c.params })),
                toolResults: response.toolResults.map((r) => r.result),
                text: response.text,
                finishReason: response.finishReason,
                usage: { in: response.usage.inputTokens.total, out: response.usage.outputTokens.total },
                neurons: finish ? Cloudflare.AI.finishNeurons(finish as never) : undefined,
              });
              prompt = Prompt.concat(prompt, Prompt.fromResponseParts(response.content));
              if (response.toolCalls.length === 0) break;
            }
            return { model: model || DEFAULT_MODEL, provider, rounds };
          }).pipe(
            Effect.catchCause((cause) => Effect.succeed({ error: String(cause), model, provider })),
          ),

        // L7: stream text back over DO RPC.
        stream: (text: string, model: string) =>
          LanguageModel.streamText({ prompt: text }).pipe(
            Stream.filter((p) => p.type === "text-delta"),
            Stream.map((p) => (p as { delta: string }).delta),
            Stream.provide(ai.model({ model: model || DEFAULT_MODEL })),
          ),

        // L7 diagnosis: the same deltas, collected inside the DO (no stream over RPC).
        streamCollect: (text: string, model: string) =>
          LanguageModel.streamText({ prompt: text }).pipe(
            Stream.filter((p) => p.type === "text-delta"),
            Stream.map((p) => (p as { delta: string }).delta),
            Stream.runCollect,
            Effect.map((chunk) => [...chunk]),
            Effect.provide(ai.model({ model: model || DEFAULT_MODEL })),
            Effect.orDie,
          ),

        // L7 diagnosis: Workers AI's own stream, bypassing the adapter.
        rawStream: (text: string) =>
          Effect.gen(function* () {
            const raw = yield* ai.raw;
            const body = (yield* Effect.promise(() =>
              raw.run(DEFAULT_MODEL as never, { messages: [{ role: "user", content: text }], stream: true } as never),
            )) as unknown as ReadableStream<Uint8Array>;
            const sse: string = yield* Effect.promise(() => new globalThis.Response(body).text());
            return sse
              .split("\n\n")
              .filter((e: string) => e.startsWith("data: ") && !e.includes("[DONE]"))
              .map((e: string) => (JSON.parse(e.slice(6)) as { response?: unknown }).response ?? "");
          }),

        // L10b diagnosis: the raw binding with a model id, bypassing the adapter.
        rawRun: (model: string) =>
          Effect.gen(function* () {
            const raw = yield* ai.raw;
            return yield* Effect.tryPromise(() =>
              raw.run(model as never, { messages: [{ role: "user", content: "Say hi." }] } as never),
            ).pipe(
              Effect.map((out) => ({ outcome: "succeeded", out: JSON.stringify(out).slice(0, 300) })),
              Effect.catch((e) => Effect.succeed({ outcome: "threw", error: String((e as { cause?: unknown }).cause ?? e).slice(0, 300) })),
            );
          }),

        // North-star dependency: structured output (Agent.prompt with an output schema).
        structured: (model: string) =>
          LanguageModel.generateObject({
            prompt: "Review this article and give a verdict: 'The council passed the budget on Tuesday by a vote of 7 to 2.'",
            schema: Schema.Struct({ approved: Schema.Boolean, notes: Schema.String }),
          }).pipe(
            Effect.provide(ai.model({ model: model || DEFAULT_MODEL, parameters: { temperature: 0 } })),
            Effect.map((r) => ({ outcome: "succeeded", value: r.value })),
            Effect.catchCause((cause) => Effect.succeed({ outcome: "failed", cause: String(cause).slice(0, 500) })),
          ),

        // Workaround: structured output as a forced "respond" tool call, validated by us.
        structuredViaTool: (model: string) =>
          Effect.gen(function* () {
            const Verdict = Schema.Struct({ approved: Schema.Boolean, notes: Schema.String });
            const Respond = Tool.make("respond", { description: "Give your final answer.", parameters: Verdict });
            const response = yield* LanguageModel.generateText({
              prompt: "Review this article and give a verdict: 'The council passed the budget on Tuesday by a vote of 7 to 2.'",
              toolkit: Toolkit.make(Respond),
              toolChoice: { tool: "respond" },
              disableToolCallResolution: true,
              toolCallValidation: "deferred",
            }).pipe(Effect.provide(ai.model({ model: model || DEFAULT_MODEL, parameters: { temperature: 0 } })));
            const call = response.toolCalls[0];
            if (!call) return { outcome: "no tool call", text: response.text };
            const value = yield* Schema.decodeUnknownEffect(Verdict)(call.params).pipe(
              Effect.map((v) => ({ outcome: "succeeded", value: v })),
              Effect.catch((issue) => Effect.succeed({ outcome: "invalid", params: call.params, issue: issue.message })),
            );
            return value;
          }).pipe(Effect.catchCause((cause) => Effect.succeed({ outcome: "failed", cause: String(cause).slice(0, 400) }))),

        // L10a: a tool call with arguments that don't match the schema.
        badArgs: (mode: "resolve" | "strict" | "deferred") =>
          LanguageModel.generateText({
            prompt: "status of order 42?",
            toolkit: Tools,
            ...(mode === "resolve"
              ? {}
              : { disableToolCallResolution: true, toolCallValidation: mode }),
          }).pipe(
            Effect.provide(Layer.merge(scripted(BAD_ARGS_SCRIPT), handlers)),
            Effect.map((r) => ({
              outcome: "succeeded",
              toolCalls: r.toolCalls.map((c) => c.params),
              toolResults: r.toolResults.map((t) => ({ isFailure: t.isFailure, result: t.result })),
            })),
            Effect.catchCause((cause) => Effect.succeed({ outcome: "failed", cause: String(cause).slice(0, 600) })),
          ),

        // L10b: a provider error (nonexistent model).
        badModel: () =>
          LanguageModel.generateText({ prompt: "hi" }).pipe(
            Effect.provide(ai.model({ model: "@cf/nope/does-not-exist" })),
            Effect.map((r) => ({
              outcome: "succeeded",
              text: r.text,
              finishReason: r.finishReason,
              parts: r.content.map((p) => ({
                type: p.type,
                ...("text" in p ? { text: String(p.text) } : {}),
                ...("error" in p ? { error: String((p as { error: unknown }).error) } : {}),
              })),
            })),
            Effect.catchCause((cause) => Effect.succeed({ outcome: "failed", cause: String(cause).slice(0, 800) })),
          ),
      };
    });
  }).pipe(Effect.provide(Layer.mergeAll(Cloudflare.Workers.AIBinding, Cloudflare.AI.QueryGatewayBinding))),
) {}
