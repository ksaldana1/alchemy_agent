// Type tests. Checked by `bun run typecheck`. Every @ts-expect-error is checked one at a time
// (TypeScript 7 can hide later errors in a file while earlier ones are present).
// Sections 1–4: the agreed API's call sites. Section 5: ported from mini-agent-runtime's
// tests/agent.test.ts ("defineAgent types").
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as Layer from "effect/Layer";
import { expectTypeOf } from "expect-type";
import * as Agent from "../../src/Agent.ts";
import * as Api from "../../src/Api.ts";

class Hiccup extends Data.TaggedError("Hiccup")<{ readonly message: string }> {}

// ===========================================================================
// 1. Definitions
// ===========================================================================

// The common case: tasks directly, input as struct fields.
export class Greeter extends Agent.make<Greeter>()("Greeter", {
  greet: Agent.task({ name: Schema.String }, ({ name }) =>
    Effect.succeed({ greeting: `Hello, ${name}!` }),
  ),
  // No input at all.
  ping: Agent.task(() => Effect.succeed("pong")),
  // Options sit between the input and the handler.
  stamp: Agent.task({ epochMs: Schema.Number }, { output: Schema.Struct({ at: Schema.DateFromString }) }, ({ epochMs }) =>
    Effect.succeed({ at: new Date(epochMs) }),
  ),
  flaky: Agent.task({}, { retry: { maxAttempts: 5 } }, () =>
    Effect.gen(function* () {
      const { attempt } = yield* Agent.AgentContext;
      if (attempt === 1) return yield* new Hiccup({ message: "first attempt" });
      return { attempt };
    }),
  ),
}) {}

// With agent-level options: `{ ...options, tasks }`. Relay sends to Greeter, so it declares it.
export class Relay extends Agent.make<Relay>()("Relay", {
  uses: [Greeter],
  retry: { maxAttempts: 3 },
  tasks: {
    forward: Agent.task({ name: Schema.String }, ({ name }) =>
      Effect.gen(function* () {
        const { taskId } = yield* Agent.AgentContext;
        // Inside a handler: send, don't wait (waiting arrives with M3).
        const handle = yield* Greeter.named("desk").send.greet({ name }, { taskId: `${taskId}:greet` });
        return { forwarded: handle.taskId };
      }),
    ),
  },
}) {}

// ===========================================================================
// 2. Calling an agent: the same reference everywhere
// ===========================================================================

const greeter = Greeter.named("a");

// Send and wait: the output, typed from the definition.
const greeting = greeter.greet({ name: "Ada" });
expectTypeOf(greeting).toEqualTypeOf<
  Effect.Effect<
    { greeting: string },
    Agent.TaskFailed<never> | Agent.AgentUnreachable,
    Agent.Reach<"Greeter"> | Agent.Waiter
  >
>();
expectTypeOf(greeter.ping()).toEqualTypeOf<
  Effect.Effect<string, Agent.TaskFailed<never> | Agent.AgentUnreachable, Agent.Reach<"Greeter"> | Agent.Waiter>
>();
expectTypeOf(greeter.stamp({ epochMs: 0 })).toEqualTypeOf<
  Effect.Effect<
    { readonly at: Date },
    Agent.TaskFailed<never> | Agent.AgentUnreachable,
    Agent.Reach<"Greeter"> | Agent.Waiter
  >
>();
// The handler's error type flows to the caller.
expectTypeOf(greeter.flaky({})).toEqualTypeOf<
  Effect.Effect<{ attempt: number }, Agent.TaskFailed<Hiccup> | Agent.AgentUnreachable, Agent.Reach<"Greeter"> | Agent.Waiter>
>();

// Fire and forget, with an idempotent task id; pick it up later by id.
const handle = greeter.send.greet({ name: "Ada" }, { taskId: "welcome-ada" });
expectTypeOf(handle).toEqualTypeOf<
  Effect.Effect<Agent.TaskHandle<typeof Greeter, "greet">, Agent.AgentUnreachable, Agent.Reach<"Greeter">>
>();
expectTypeOf(greeter.handle("greet", "welcome-ada").result).toEqualTypeOf<
  Effect.Effect<
    { greeting: string },
    Agent.TaskFailed<never> | Agent.TaskNotFound | Agent.AgentUnreachable,
    Agent.Reach<"Greeter"> | Agent.Waiter
  >
>();

// Addresses are data: "share with that agent".
expectTypeOf(greeter.address).toEqualTypeOf<Agent.Address<"Greeter">>();

// @ts-expect-error unknown task
greeter.nope({});
// @ts-expect-error wrong input
greeter.greet({ name: 1 });
// @ts-expect-error missing input
greeter.greet();

// ===========================================================================
// 3. Providing "how to reach agents": a Worker, and an HTTP client
// ===========================================================================

// In a Worker: connect in the construction phase, provide in fetch. The API is mountable.
export const workerBody = Effect.gen(function* () {
  const agents = yield* Agent.connect(Greeter, Relay);
  const api = yield* Api.routes({ agents: [Greeter, Relay] });
  return {
    fetch: Effect.gen(function* () {
      const fromApi = yield* api.handle;
      if (fromApi) return fromApi;
      // Your own routes call agents directly.
      return yield* Effect.map(Greeter.named("home").greet({ name: "visitor" }), (g) => g.greeting);
    }).pipe(Effect.provide(agents)),
  };
});
expectTypeOf<Effect.Success<typeof workerBody>["fetch"]>().toExtend<Effect.Effect<unknown, unknown, unknown>>();

// Outside Cloudflare (a script or test): the same calls, over HTTP.
const viaHttp = Api.connect({ url: "https://…", token: "…" }, Greeter);
expectTypeOf(viaHttp).toExtend<Layer.Layer<Agent.Reach<"Greeter"> | Agent.Waiter, never, any>>();
export const script = greeter.greet({ name: "Ada" }).pipe(Effect.provide(viaHttp));

// ===========================================================================
// 4. Compile-time checks on definitions
// ===========================================================================

Agent.make<unknown>()("NotJson", {
  // @ts-expect-error Task "t" returns a value that isn't plain JSON; add an output schema
  t: Agent.task(() => Effect.succeed(new Date())),
});

Agent.make<unknown>()("Reserved", {
  // @ts-expect-error "send" can't be a task name (reserved by agent references)
  send: Agent.task(() => Effect.void),
});

Agent.make<unknown>()("Undeclared", {
  // @ts-expect-error Task "t" needs a service the framework does not provide (Greeter isn't in `uses`)
  t: Agent.task(() => Effect.asVoid(Greeter.named("x").send.ping())),
});

Agent.make<unknown>()("Waits", {
  uses: [Greeter],
  tasks: {
    // @ts-expect-error Task "t" waits for another agent; use send.<task> until M3
    t: Agent.task(() => Greeter.named("x").ping()),
  },
});

// ===========================================================================
// 5. Ported from mini-agent-runtime: inference from schemas and handlers
// ===========================================================================

class Editor extends Agent.make<Editor>()("Editor", {
  edit: Agent.task(
    {
      path: Schema.String,
      name: Schema.String,
      shout: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
    },
    (input) => {
      expectTypeOf(input).toEqualTypeOf<{ readonly path: string; readonly name: string; readonly shout: boolean }>();
      return Effect.succeed({ path: `~/edited/${input.name}.txt`, bytes: 3 });
    },
  ),
  count: Agent.task({ items: Schema.Array(Schema.String) }, ({ items }) => Effect.succeed(items.length)),
  nothing: Agent.task(() => Effect.void),
}) {}

expectTypeOf<Agent.TaskNames<typeof Editor>>().toEqualTypeOf<"edit" | "count" | "nothing">();
expectTypeOf<Agent.AgentType<typeof Editor>>().toEqualTypeOf<"Editor">();
// On the caller side, fields with a decoding default are optional.
expectTypeOf<Agent.TaskInput<typeof Editor, "edit">>().toEqualTypeOf<{
  readonly path: string;
  readonly name: string;
  readonly shout?: boolean;
}>();
expectTypeOf<Agent.TaskInputDecoded<typeof Editor, "edit">>().toEqualTypeOf<{
  readonly path: string;
  readonly name: string;
  readonly shout: boolean;
}>();
expectTypeOf<Agent.TaskInput<typeof Editor, "count">>().toEqualTypeOf<{ readonly items: ReadonlyArray<string> }>();
expectTypeOf<Agent.TaskOutput<typeof Editor, "edit">>().toEqualTypeOf<{ path: string; bytes: number }>();
expectTypeOf<Agent.TaskOutput<typeof Editor, "count">>().toEqualTypeOf<number>();
expectTypeOf<Agent.TaskOutput<typeof Editor, "nothing">>().toEqualTypeOf<void>();

Agent.make<unknown>()("BadField", {
  t: Agent.task({ a: Schema.String }, (input) =>
    // @ts-expect-error `b` is not in the schema
    Effect.succeed(input.b),
  ),
});
