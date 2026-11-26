// Models for `Agent.generate`, as Layers that provide Effect AI's `LanguageModel`:
//
//   DeskLive.pipe(Layer.provide(AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast")))
//
// Any Effect AI provider Layer works the same way. The Cloudflare models here attach their
// binding while the Worker is set up and build the model inside each step, where Alchemy's
// runtime context exists (its Workers AI adapter needs it).
import * as Cloudflare from "@alchemy.run/cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as LanguageModel from "effect/ai/LanguageModel";
import type * as Prompt from "effect/ai/Prompt";
import type * as Response from "effect/ai/Response";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

export interface ModelParameters {
  readonly temperature?: number;
  readonly maxTokens?: number;
  readonly topP?: number;
  readonly seed?: number;
}

/** @internal A model that has to be built inside each step (see the module comment). */
export class DeferredModel extends Context.Service<
  DeferredModel,
  { readonly layer: Layer.Layer<LanguageModel.LanguageModel, never, RuntimeContext> }
>()("alchemy-agent/DeferredModel") {}

const deferred = (build: Effect.Effect<Layer.Layer<LanguageModel.LanguageModel, never, RuntimeContext>, never, any>) =>
  Layer.effect(DeferredModel, Effect.map(build, (layer) => DeferredModel.of({ layer }))) as unknown as Layer.Layer<
    LanguageModel.LanguageModel,
    never,
    Cloudflare.Worker
  >;

/** Workers AI, called directly through the `AI` binding. */
export const workersAI = (model: string, parameters: ModelParameters = { temperature: 0 }) =>
  deferred(
    Effect.gen(function* () {
      const ai = yield* Cloudflare.Workers.AI();
      return ai.model({ model, parameters });
    }).pipe(Effect.provide(Cloudflare.Workers.AIBinding)),
  );

/**
 * A Workers AI model routed through an AI Gateway (caching, rate limits, logs). Alchemy's adapter
 * routes Workers AI models only (docs/spike/0.5-llm.md, L6).
 */
export const gateway = (
  gateway: ArgsOf<typeof Cloudflare.AI.QueryGateway>[0],
  model: string,
  parameters: ModelParameters = { temperature: 0 },
) =>
  deferred(
    Effect.gen(function* () {
      const client = yield* Cloudflare.AI.QueryGateway(gateway);
      return client.model({ model, parameters });
    }).pipe(Effect.provide(Cloudflare.AI.QueryGatewayBinding)),
  );
type ArgsOf<F> = F extends (...args: infer A) => unknown ? A : never;

// ---------------------------------------------------------------------------
// Scripted models (tests)
// ---------------------------------------------------------------------------

/** One scripted model response: the parts the model "returns". */
export type ScriptedResponse = ReadonlyArray<Response.PartEncoded>;

const finish = (reason: "stop" | "tool-calls"): Response.PartEncoded =>
  ({ type: "finish", reason, usage: { inputTokens: { total: 10 }, outputTokens: { total: 5 } } }) as never;

/** A plain text answer. */
export const text = (value: string): ScriptedResponse => [{ type: "text", text: value } as Response.PartEncoded, finish("stop")];

/** One or more tool calls. Ids are derived from the position, so replays are identical. */
export const toolCall = (name: string, params: unknown, ...more: ReadonlyArray<readonly [string, unknown]>): ScriptedResponse => [
  ...[[name, params] as const, ...more].map(
    ([n, p], i) => ({ type: "tool-call", id: `call_${n}_${i}`, name: n, params: p, providerExecuted: false }) as Response.PartEncoded,
  ),
  finish("tool-calls"),
];

/** A structured answer (the `respond` tool `Agent.generate` uses for non-string schemas). */
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
): Layer.Layer<LanguageModel.LanguageModel> =>
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
  );
