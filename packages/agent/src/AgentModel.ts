// Models for LLM agents. A model is a spec the agent's Durable Object turns into an Effect AI
// `LanguageModel` Layer in its construction phase (where Alchemy allows bindings).
//
//   model: AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast")
//   model: AgentModel.scripted([AgentModel.toolCall("lookup_order", { orderId: "42" }), AgentModel.text("Shipped.")])
import * as Cloudflare from "@alchemy.run/cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as LanguageModel from "effect/ai/LanguageModel";
import type * as Prompt from "effect/ai/Prompt";
import type * as Response from "effect/ai/Response";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

export interface Parameters {
  readonly temperature?: number;
  readonly maxTokens?: number;
  readonly topP?: number;
  readonly seed?: number;
}

/** A model, resolved in the agent Durable Object's construction phase. */
export interface AgentModel {
  readonly _tag: "AgentModel";
  readonly description: string;
  /** Runs in the construction phase; returns the Layer each step provides. */
  readonly build: Effect.Effect<Layer.Layer<LanguageModel.LanguageModel, never, RuntimeContext>, never, any>;
}

/** Workers AI, called directly through the `AI` binding. */
export const workersAI = (model: string, parameters: Parameters = { temperature: 0 }): AgentModel => ({
  _tag: "AgentModel",
  description: `workers-ai:${model}`,
  build: Effect.gen(function* () {
    const ai = yield* Cloudflare.Workers.AI();
    return ai.model({ model, parameters });
  }).pipe(Effect.provide(Cloudflare.Workers.AIBinding)),
});

/**
 * A Workers AI model routed through an AI Gateway (caching, rate limits, logs).
 * Alchemy's adapter routes Workers AI models only (docs/spike/0.5-llm.md, L6).
 */
export const gateway = (
  gateway: Parameters_<typeof Cloudflare.AI.QueryGateway>[0],
  model: string,
  parameters: Parameters = { temperature: 0 },
): AgentModel => ({
  _tag: "AgentModel",
  description: `gateway:${model}`,
  build: Effect.gen(function* () {
    const client = yield* Cloudflare.AI.QueryGateway(gateway);
    return client.model({ model, parameters });
  }).pipe(Effect.provide(Cloudflare.AI.QueryGatewayBinding)),
});
type Parameters_<F> = F extends (...args: infer A) => unknown ? A : never;

// ---------------------------------------------------------------------------
// Scripted models (tests)
// ---------------------------------------------------------------------------

/** One scripted model response: the parts the model "returns". */
export type ScriptedResponse = ReadonlyArray<Response.PartEncoded>;

const finish = (reason: "stop" | "tool-calls"): Response.PartEncoded =>
  ({ type: "finish", reason, usage: { inputTokens: { total: 10 }, outputTokens: { total: 5 } } }) as never;

/** A plain text answer. */
export const text = (value: string): ScriptedResponse =>
  [{ type: "text", text: value } as Response.PartEncoded, finish("stop")];

/** One or more tool calls. Ids are derived from the step, so replays are identical. */
export const toolCall = (name: string, params: unknown, ...more: ReadonlyArray<readonly [string, unknown]>): ScriptedResponse => [
  ...[[name, params] as const, ...more].map(
    ([n, p], i) => ({ type: "tool-call", id: `call_${n}_${i}`, name: n, params: p, providerExecuted: false }) as Response.PartEncoded,
  ),
  finish("tool-calls"),
];

/** A structured final answer (the `respond` tool `Agent.prompt` uses for `output`). */
export const respond = (value: unknown): ScriptedResponse => toolCall("respond", value);

/** An empty response: what Alchemy's adapter returns for a provider error (spike 0.5, L10b). */
export const empty: ScriptedResponse = [finish("stop")];

/**
 * A deterministic model for tests. Answer `n` is used when the prompt already holds `n`
 * assistant messages, so the same prompt always gets the same answer (safe to replay). Pass a
 * function for answers that depend on the prompt.
 */
export const scripted = (
  script: ReadonlyArray<ScriptedResponse> | ((prompt: Prompt.Prompt) => ScriptedResponse),
): AgentModel => ({
  _tag: "AgentModel",
  description: "scripted",
  build: Effect.succeed(
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: (options) =>
          Effect.sync(() => {
            if (typeof script === "function") return script(options.prompt) as never;
            const n = options.prompt.content.filter((m) => m.role === "assistant").length;
            return (script[n] ?? text("(script exhausted)")) as never;
          }),
        streamText: () => Stream.empty,
      }),
    ),
  ),
});
