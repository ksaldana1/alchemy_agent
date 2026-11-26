// API reset prototype: how the reset API reads at its call sites. Type-checked, never run.
import * as Tool from "effect/ai/Tool";
import * as Toolkit from "effect/ai/Toolkit";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";
import { expectTypeOf } from "expect-type";
import * as Agent from "./Agent.ts";

// ===========================================================================
// 1. Contracts (importable anywhere; no runtime code)
// ===========================================================================

export class UnknownCustomer extends Schema.TaggedError<UnknownCustomer>()("UnknownCustomer", {
  name: Schema.String,
}) {}

export const Greet = Rpc.make("Greet", {
  payload: { name: Schema.String },
  success: Schema.Struct({ greeting: Schema.String }),
  error: UnknownCustomer,
  primaryKey: ({ name }) => name,
});
export const Ping = Rpc.make("Ping", { success: Schema.String });

export const Greeter = Agent.make("Greeter", [Greet, Ping]);

export const Forward = Rpc.make("Forward", { payload: { name: Schema.String } });
export const Relay = Agent.make("Relay", [Forward]);

// ===========================================================================
// 2. Implementations (Layers)
// ===========================================================================

export const GreeterLive = Greeter.toLayer(
  Greeter.of({
    Greet: ({ payload, attempt }) =>
      payload.name === "nobody"
        ? Effect.fail(new UnknownCustomer({ name: payload.name }))
        : Effect.succeed({ greeting: `Hello, ${payload.name}! (attempt ${attempt})` }),
    Ping: () => Effect.succeed("pong"),
  }),
  { retry: { maxAttempts: 3 } },
);
expectTypeOf(GreeterLive).toEqualTypeOf<Layer.Layer<Agent.AgentHost<"Greeter">, never, never>>();

// Other agents: yield their client while building; send without waiting (until M3).
export const RelayLive = Relay.toLayer(
  Effect.gen(function* () {
    const greeters = yield* Greeter.client;
    return Relay.of({
      Forward: Effect.fn("Relay.Forward")(function* ({ payload, taskId }) {
        yield* greeters("desk").Greet({ name: payload.name }, { discard: true });
        void taskId;
      }),
    });
  }),
);
// Reaching other agents is provided by the framework where the agent runs: no requirement left.
expectTypeOf(RelayLive).toEqualTypeOf<Layer.Layer<Agent.AgentHost<"Relay">, never, never>>();

// ===========================================================================
// 3. An LLM agent: the model and the tool handlers are Layers
// ===========================================================================

const LookupOrder = Tool.make("LookupOrder", {
  description: "Look up an order by id.",
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});
const Tools = Toolkit.make(LookupOrder);
const ToolsLive = Tools.toLayer(Tools.of({ LookupOrder: () => Effect.succeed({ status: "shipped" }) }));

export const Verdict = Schema.Struct({ approved: Schema.Boolean, notes: Schema.String });
export const Ask = Rpc.make("Ask", { payload: { question: Schema.String }, success: Schema.String });
export const Review = Rpc.make("Review", { payload: { article: Schema.String }, success: Verdict });
export const Desk = Agent.make("Desk", [Ask, Review]);

export const DeskLive = Desk.toLayer(
  Desk.of({
    Ask: ({ payload }) => Agent.generate({ prompt: payload.question, toolkit: Tools, system: "Be brief." }),
    Review: ({ payload }) => Agent.generate({ prompt: `Review: ${payload.article}`, schema: Verdict }),
  }),
).pipe(Layer.provide([Agent.AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast"), ToolsLive]));
expectTypeOf(DeskLive).toEqualTypeOf<Layer.Layer<Agent.AgentHost<"Desk">, never, never>>();

// Without the tool handlers, the Layer says what's missing.
const DeskNoTools = Desk.toLayer(
  Desk.of({
    Ask: ({ payload }) => Agent.generate({ prompt: payload.question, toolkit: Tools }),
    Review: ({ payload }) => Agent.generate({ prompt: payload.article, schema: Verdict }),
  }),
).pipe(Layer.provide(Agent.AgentModel.workersAI("m")));
expectTypeOf<Layer.Services<typeof DeskNoTools>>().toEqualTypeOf<Tool.HandlersFor<{ LookupOrder: typeof LookupOrder }>>();

// One `generate`, typed by its schema.
expectTypeOf(Agent.generate({ prompt: "hi" })).toEqualTypeOf<
  Effect.Effect<string, Agent.GenerationError, import("effect/ai/LanguageModel").LanguageModel>
>();
expectTypeOf(Agent.generate({ prompt: "verdict?", schema: Verdict })).toEqualTypeOf<
  Effect.Effect<{ readonly approved: boolean; readonly notes: string }, Agent.GenerationError, import("effect/ai/LanguageModel").LanguageModel>
>();
expectTypeOf(Agent.generate({ prompt: "pick one", schema: Schema.Literals(["approve", "reject"]) })).toEqualTypeOf<
  Effect.Effect<"approve" | "reject", Agent.GenerationError, import("effect/ai/LanguageModel").LanguageModel>
>();
expectTypeOf(Agent.generate({ prompt: "how many?", schema: Schema.NumberFromString })).toEqualTypeOf<
  Effect.Effect<number, Agent.GenerationError, import("effect/ai/LanguageModel").LanguageModel>
>();

// ===========================================================================
// 4. Clients
// ===========================================================================

export const calls = Effect.gen(function* () {
  const greeters = yield* Greeter.client;
  const greeter = greeters("a");

  // Waits for the answer. Declared errors arrive as themselves; platform failures as AgentError.
  const greeting = greeter.Greet({ name: "Ada" });
  expectTypeOf(greeting).toEqualTypeOf<
    Effect.Effect<{ readonly greeting: string }, UnknownCustomer | Agent.AgentError, Agent.Waiter>
  >();

  // Fire and forget.
  expectTypeOf(greeter.Greet({ name: "Ada" }, { discard: true })).toEqualTypeOf<Effect.Effect<void, Agent.AgentError>>();

  // A task without a payload.
  expectTypeOf(greeter.Ping()).toEqualTypeOf<Effect.Effect<string, Agent.AgentError, Agent.Waiter>>();

  // Declared errors are plain catchTag.
  const recovered = greeting.pipe(Effect.catchTag("UnknownCustomer", () => Effect.succeed({ greeting: "?" } as const)));
  expectTypeOf(recovered).toEqualTypeOf<
    Effect.Effect<{ readonly greeting: string } | { readonly greeting: "?" }, Agent.AgentError, Agent.Waiter>
  >();

  // @ts-expect-error unknown task
  greeter.Nope({});
  // @ts-expect-error wrong payload
  greeter.Greet({ name: 1 });
  return yield* greeting;
});
expectTypeOf(calls).toEqualTypeOf<
  Effect.Effect<{ readonly greeting: string }, UnknownCustomer | Agent.AgentError, Agent.Agents | Agent.Waiter>
>();

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

// A handler that waits for another agent: the Layer carries an unprovidable requirement that
// says why, so providing it fails to compile.
const Waits = Relay.toLayer(
  Effect.gen(function* () {
    const greeters = yield* Greeter.client;
    return Relay.of({ Forward: ({ payload }) => Effect.asVoid(greeters("x").Greet({ name: payload.name })) });
  }),
);
expectTypeOf<Layer.Services<typeof Waits>>().toEqualTypeOf<Agent.WaitingInHandler>();
