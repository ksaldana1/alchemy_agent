// Spike 0.2 type tests. Checked by `tsc -b` (bun run typecheck); bun's test runner doesn't
// check types, and this file doesn't match bun's *.test.ts pattern.
import type * as Cloudflare from "@alchemy.run/cloudflare";
import * as CF from "@alchemy.run/cloudflare";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { expectTypeOf } from "expect-type";
import * as Agent from "../../src/agent.ts";
import type {
  AgentClient,
  TaskError,
  TaskInput,
  TaskInputDecoded,
  TaskNames,
  TaskOutput,
  TaskRecord,
} from "../../src/agent.ts";
import type Echo from "../../src/echo.ts";
import type Relay from "../../src/relay.ts";

// --- Helper types read the definition ---------------------------------------------------

expectTypeOf<TaskNames<typeof Echo>>().toEqualTypeOf<"echo" | "count">();
expectTypeOf<TaskInput<typeof Echo, "echo">>().toEqualTypeOf<{ readonly text: string }>();
expectTypeOf<TaskOutput<typeof Echo, "echo">>().toEqualTypeOf<{ echoed: string }>();
expectTypeOf<TaskOutput<typeof Echo, "count">>().toEqualTypeOf<{ n: number; by: string }>();
expectTypeOf<TaskNames<typeof Relay>>().toEqualTypeOf<"relay" | "fail" | "stamp">();

// Encoded vs decoded: callers pass Encoded, handlers get Type; outputs with a schema are Type.
expectTypeOf<TaskInput<typeof Relay, "stamp">>().toEqualTypeOf<{ readonly epochMs: number }>();
expectTypeOf<TaskInputDecoded<typeof Relay, "stamp">>().toEqualTypeOf<{
  readonly epochMs: number;
}>();
expectTypeOf<TaskOutput<typeof Relay, "stamp">>().toEqualTypeOf<{ readonly at: Date }>();

// Errors stay tagged in the type.
expectTypeOf<TaskError<typeof Relay, "fail">>().toEqualTypeOf<Agent.PermanentError>();
expectTypeOf<TaskError<typeof Echo, "echo">>().toEqualTypeOf<never>();

// Agent B's reference to agent A is typed end to end.
expectTypeOf<TaskOutput<typeof Relay, "relay">>().toEqualTypeOf<{
  via: string;
  record: TaskRecord<{ echoed: string }, never> | undefined;
}>();

// --- Approach C: the definition-typed client --------------------------------------------

declare const client: AgentClient<typeof Echo>;
expectTypeOf(client.send("t1", "echo", { text: "hi" })).toExtend<
  Effect.Effect<Agent.TaskHandle<"echo">, Agent.TaskInputError | Agent.UnknownTaskError, any>
>();
// @ts-expect-error unknown task
client.send("t1", "nope", {});
// @ts-expect-error wrong input
client.send("t1", "echo", { text: 1 });

declare const relayClient: AgentClient<typeof Relay>;
declare const stampHandle: Agent.TaskHandle<"stamp">;
expectTypeOf(relayClient.result(stampHandle)).toExtend<
  Effect.Effect<TaskRecord<{ readonly at: Date }, never> | undefined, never, any>
>();

// --- Approach A: Alchemy's stub keeps the generic `enqueue` --------------------------------

declare const echoStub: Cloudflare.DurableObjectStub<Echo>;
expectTypeOf(echoStub.enqueue("t1", "echo", { text: "hi" })).toExtend<
  Effect.Effect<Agent.TaskHandle<"echo">, Agent.TaskInputError | Agent.UnknownTaskError, any>
>();
// @ts-expect-error unknown task through the raw stub
echoStub.enqueue("t1", "nope", {});
// @ts-expect-error wrong input through the raw stub
echoStub.enqueue("t1", "echo", { text: 1 });

// The namespace you get from `yield* Echo` hands out that same stub type.
declare const echoNamespace: Effect.Success<typeof import("../../src/echo.ts").default>;
expectTypeOf(echoNamespace.getByName("a")).toEqualTypeOf<Cloudflare.DurableObjectStub<Echo>>();

// --- Approach B: nested RPC objects are rejected by Alchemy's `MainRpc` constraint ----------

class Nested extends CF.DurableObject<Nested>()(
  "Nested",
  // @ts-expect-error `tasks: { ... }` is not an Effect, a Stream, or a function returning one
  Effect.succeed(Effect.succeed({ tasks: { echo: () => Effect.succeed(1) } })),
) {}
void Nested;

// Flat mapped methods (approach B without nesting) do keep their types.
class Flat extends CF.DurableObject<Flat>()(
  "Flat",
  Effect.succeed(
    Effect.succeed({
      task_echo: (taskId: string, input: { readonly text: string }) =>
        Effect.succeed({ taskId, echoed: input.text }),
    }),
  ),
) {}
declare const flatStub: Cloudflare.DurableObjectStub<Flat>;
expectTypeOf(flatStub.task_echo).parameter(1).toEqualTypeOf<{ readonly text: string }>();

// --- A handler that needs a service the framework doesn't provide --------------------------

class Workspace extends Context.Service<Workspace, { readonly read: string }>()("Workspace") {}

Agent.make<unknown>()("Bad", {
  tasks: {
    // @ts-expect-error Task "t" needs a service the framework does not provide
    t: Agent.task({ input: Schema.Struct({}) }, () =>
      Effect.gen(function* () {
        return (yield* Workspace).read;
      }),
    ),
  },
});

// Declaring the agent in `uses` is what allows `Agent.use(Echo)`.
Agent.make<unknown>()("NoUses", {
  tasks: {
    // @ts-expect-error Agent.use(Echo) without `uses: [Echo]`
    t: Agent.task({ input: Schema.Struct({}) }, () =>
      Agent.use(null as unknown as typeof import("../../src/echo.ts").default),
    ),
  },
});
