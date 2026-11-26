// Type tests for the public API. Checked by `bun run typecheck`. Every @ts-expect-error is checked
// one at a time (TypeScript 7 can hide later errors in a file while earlier ones are present).
import type * as Cloudflare from "@alchemy.run/cloudflare";
import type * as LanguageModel from "effect/ai/LanguageModel";
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";
import { expectTypeOf } from "expect-type";
import * as Agent from "../../src/Agent.ts";
import * as AgentModel from "../../src/AgentModel.ts";

// ===========================================================================
// 1. Contracts
// ===========================================================================

class UnknownCustomer extends Schema.TaggedError<UnknownCustomer>()("UnknownCustomer", { name: Schema.String }) {}

const Greet = Rpc.make("Greet", {
  payload: { name: Schema.String },
  success: Schema.Struct({ greeting: Schema.String }),
  error: UnknownCustomer,
  primaryKey: ({ name }) => name,
});
const Ping = Rpc.make("Ping", { success: Schema.String });
const Greeter = Agent.make("Greeter", [Greet, Ping]);

const Forward = Rpc.make("Forward", { payload: { name: Schema.String } });
const Relay = Agent.make("Relay", [Forward]);

expectTypeOf(Greeter.type).toEqualTypeOf<"Greeter">();
expectTypeOf(Greeter.address("a")).toEqualTypeOf<Agent.Address<"Greeter">>();

// ===========================================================================
// 2. Implementations
// ===========================================================================

const GreeterLive = Greeter.toLayer(
  Greeter.of({
    Greet: ({ payload, attempt, agent }) =>
      payload.name === "nobody"
        ? Effect.fail(new UnknownCustomer({ name: payload.name }))
        : Effect.succeed({ greeting: `Hello, ${payload.name}! (${agent.name}, attempt ${attempt})` }),
    Ping: () => Effect.succeed("pong"),
  }),
  { retry: { maxAttempts: 3 } },
);
// The hosting Worker provides Alchemy's `Worker`; nothing else is needed.
expectTypeOf(GreeterLive).toEqualTypeOf<Layer.Layer<Agent.AgentHost<"Greeter">, never, Cloudflare.Worker>>();

// Other agents: yield their client while building. It adds no requirement.
const RelayLive = Relay.toLayer(
  Effect.gen(function* () {
    const greeters = yield* Greeter.client;
    return Relay.of({
      Forward: ({ payload }) => greeters("desk").Greet({ name: payload.name }, { discard: true }),
    });
  }),
);
expectTypeOf(RelayLive).toEqualTypeOf<Layer.Layer<Agent.AgentHost<"Relay">, never, Cloudflare.Worker>>();

// Your own services become requirements of the Layer, as in Effect.
class Clock2 extends Context.Service<Clock2, { readonly now: number }>()("Clock2") {}
const WithService = Relay.toLayer(Relay.of({ Forward: () => Effect.asVoid(Effect.service(Clock2)) }));
expectTypeOf<Layer.Services<typeof WithService>>().toEqualTypeOf<Cloudflare.Worker | Clock2>();

// ===========================================================================
// 3. LLM agents: the model and the tool handlers are Layers
// ===========================================================================

const LookupOrder = Tool.make("LookupOrder", {
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});
const Tools = Toolkit.make(LookupOrder);
const ToolsLive = Tools.toLayer(Tools.of({ LookupOrder: () => Effect.succeed({ status: "shipped" }) }));

const Verdict = Schema.Struct({ approved: Schema.Boolean, notes: Schema.String });
const Ask = Rpc.make("Ask", { payload: { question: Schema.String }, success: Schema.String });
const Review = Rpc.make("Review", { payload: { article: Schema.String }, success: Verdict });
const Desk = Agent.make("Desk", [Ask, Review]);

const DeskImpl = Desk.of({
  Ask: ({ payload }) => Agent.generate({ prompt: payload.question, system: "Be brief." }),
  Review: ({ payload }) => Agent.generate({ prompt: `Review: ${payload.article}`, schema: Verdict }),
});
// Without a model or tool handlers, the Layer says what's missing…
const DeskBare = Desk.toLayer(DeskImpl, { toolkit: Tools });
expectTypeOf<Layer.Services<typeof DeskBare>>().toEqualTypeOf<
  Cloudflare.Worker | LanguageModel.LanguageModel | Tool.HandlersFor<{ LookupOrder: typeof LookupOrder }>
>();
// …and `Layer.provide` supplies it.
const DeskLive = DeskBare.pipe(Layer.provide([AgentModel.scripted([]), ToolsLive]));
expectTypeOf(DeskLive).toEqualTypeOf<Layer.Layer<Agent.AgentHost<"Desk">, never, Cloudflare.Worker>>();

// One `generate`, typed by its schema.
expectTypeOf(Agent.generate({ prompt: "hi" })).toEqualTypeOf<Effect.Effect<string, Agent.GenerationError, LanguageModel.LanguageModel>>();
expectTypeOf(Agent.generate({ prompt: "verdict?", schema: Verdict })).toEqualTypeOf<
  Effect.Effect<{ readonly approved: boolean; readonly notes: string }, Agent.GenerationError, LanguageModel.LanguageModel>
>();
expectTypeOf(Agent.generate({ prompt: "pick", schema: Schema.Literals(["approve", "reject"]) })).toEqualTypeOf<
  Effect.Effect<"approve" | "reject", Agent.GenerationError, LanguageModel.LanguageModel>
>();
expectTypeOf(Agent.generate({ prompt: "how many?", schema: Schema.FiniteFromString })).toEqualTypeOf<
  Effect.Effect<number, Agent.GenerationError, LanguageModel.LanguageModel>
>();

// ===========================================================================
// 4. Clients
// ===========================================================================

export const calls = Effect.gen(function* () {
  const greeter = (yield* Greeter.client)("a");

  // Waits for the answer. Declared errors arrive as themselves; platform failures as AgentError.
  const greeting = greeter.Greet({ name: "Ada" });
  expectTypeOf(greeting).toEqualTypeOf<Effect.Effect<{ readonly greeting: string }, UnknownCustomer | Agent.AgentError>>();
  // Fire and forget.
  expectTypeOf(greeter.Greet({ name: "Ada" }, { discard: true })).toEqualTypeOf<Effect.Effect<void, Agent.AgentError>>();
  // A task without a payload takes none.
  expectTypeOf(greeter.Ping()).toEqualTypeOf<Effect.Effect<string, Agent.AgentError>>();
  // Declared errors are plain catchTag.
  expectTypeOf(greeting.pipe(Effect.catchTag("UnknownCustomer", () => Effect.succeed({ greeting: "?" } as const)))).toEqualTypeOf<
    Effect.Effect<{ readonly greeting: string } | { readonly greeting: "?" }, Agent.AgentError>
  >();

  // @ts-expect-error unknown task
  greeter.Nope({});
  // @ts-expect-error wrong payload
  greeter.Greet({ name: 1 });
  return yield* greeting;
});
expectTypeOf(calls).toEqualTypeOf<Effect.Effect<{ readonly greeting: string }, UnknownCustomer | Agent.AgentError>>();

// ===========================================================================
// 5. Mistakes the types catch
// ===========================================================================

Greeter.of({
  // @ts-expect-error wrong success type
  Greet: () => Effect.succeed({ greeting: 1 }),
  Ping: () => Effect.succeed("pong"),
});

// @ts-expect-error a handler is missing
Greeter.of({ Greet: () => Effect.succeed({ greeting: "hi" }) });

Desk.of({
  Ask: ({ payload }) => Agent.generate({ prompt: payload.question }),
  // @ts-expect-error the generated type must match the task's success schema
  Review: ({ payload }) => Agent.generate({ prompt: payload.article }),
});
