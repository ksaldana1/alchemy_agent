// Type tests, ported from mini-agent-runtime/tests/agent.test.ts ("defineAgent types") plus the
// M1 defaults. Checked by `bun run typecheck`. Every @ts-expect-error was checked one at a time
// (TypeScript 7 can hide later errors in a file while earlier ones are present).
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type * as HttpClient from "effect/http/HttpClient";
import { expectTypeOf } from "expect-type";
import * as Agent from "../../src/Agent.ts";
import type * as Api from "../../src/Api.ts";
import type { Echo, Flaky, Relay } from "../fixtures/agents.ts";

class Editor extends Agent.make<Editor>()("Editor", {
  tasks: {
    edit: Agent.task(
      {
        input: Schema.Struct({
          path: Schema.String,
          name: Schema.String,
          shout: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
        }),
      },
      (input) => {
        expectTypeOf(input).toEqualTypeOf<{
          readonly path: string;
          readonly name: string;
          readonly shout: boolean;
        }>();
        return Effect.succeed({ path: `~/edited/${input.name}.txt`, bytes: 3 });
      },
    ),
    count: Agent.task({ input: Schema.Struct({ items: Schema.Array(Schema.String) }) }, ({ items }) =>
      Effect.succeed(items.length),
    ),
    nothing: Agent.task({ input: Schema.Struct({}) }, () => Effect.void),
  },
}) {}

// --- infers task names ---------------------------------------------------------------------
expectTypeOf<Agent.TaskNames<typeof Editor>>().toEqualTypeOf<"edit" | "count" | "nothing">();
expectTypeOf<Agent.TaskNames<typeof Echo>>().toEqualTypeOf<"echo" | "whoami" | "order" | "stamp">();
expectTypeOf<Agent.AgentType<typeof Editor>>().toEqualTypeOf<"Editor">();

// --- infers inputs from the schema; on the caller side, defaults are optional ----------------
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
expectTypeOf<Agent.TaskInput<typeof Editor, "count">>().toEqualTypeOf<{
  readonly items: ReadonlyArray<string>;
}>();

// --- infers outputs from the handler, or from the output schema ------------------------------
expectTypeOf<Agent.TaskOutput<typeof Editor, "edit">>().toEqualTypeOf<{ path: string; bytes: number }>();
expectTypeOf<Agent.TaskOutput<typeof Editor, "count">>().toEqualTypeOf<number>();
expectTypeOf<Agent.TaskOutput<typeof Editor, "nothing">>().toEqualTypeOf<void>();
expectTypeOf<Agent.TaskOutput<typeof Echo, "stamp">>().toEqualTypeOf<{ readonly at: Date }>();

// --- errors stay typed ----------------------------------------------------------------------
expectTypeOf<Agent.TaskError<typeof Flaky, "permanent">>().toEqualTypeOf<Agent.PermanentError>();

// --- the clients are typed from the definition -----------------------------------------------
declare const api: Api.Client;
const echo = api.agent({} as typeof Echo, "a");
expectTypeOf(echo.send("echo", { text: "hi" })).toEqualTypeOf<
  Effect.Effect<
    Agent.TaskHandle<typeof Echo, "echo", Api.ApiError, HttpClient.HttpClient>,
    Api.ApiError,
    HttpClient.HttpClient
  >
>();
declare const handle: Agent.TaskHandle<typeof Echo, "echo">;
expectTypeOf(handle.result()).toEqualTypeOf<
  Effect.Effect<{ echoed: string }, Agent.TaskFailed<never> | Agent.TaskNotFound>
>();
// @ts-expect-error unknown task
echo.send("nope", {});
// @ts-expect-error wrong input
echo.send("echo", { text: 1 });

// Default 1: RuntimeContext is not in user-visible types. A binding client needs nothing.
declare const lookup: Agent.AgentLookup<typeof Echo>;
expectTypeOf(lookup.getByName("a").send("echo", { text: "x" })).toEqualTypeOf<
  Effect.Effect<Agent.TaskHandle<typeof Echo, "echo">>
>();
expectTypeOf<Agent.TaskOutput<typeof Relay, "forward">>().toEqualTypeOf<{ taskId: string }>();

// --- rejects handlers that use fields the schema doesn't have ---------------------------------
Agent.make<unknown>()("BadField", {
  tasks: {
    t: Agent.task({ input: Schema.Struct({ a: Schema.String }) }, (input) =>
      // @ts-expect-error `b` is not in the schema
      Effect.succeed(input.b),
    ),
  },
});

// --- Default 2: without an output schema, the output must be plain JSON ----------------------
Agent.make<unknown>()("BadOutput", {
  tasks: {
    // @ts-expect-error Task "t" returns a value that isn't plain JSON; add an output schema
    t: Agent.task({ input: Schema.Struct({}) }, () => Effect.succeed({ at: new Date() })),
  },
});

// --- handlers may only need framework services and the agents in `uses` ----------------------
class Workspace extends Context.Service<Workspace, { readonly read: string }>()("Workspace") {}
Agent.make<unknown>()("BadService", {
  tasks: {
    // @ts-expect-error Task "t" needs a service the framework does not provide
    t: Agent.task({ input: Schema.Struct({}) }, () =>
      Effect.gen(function* () {
        return (yield* Workspace).read;
      }),
    ),
  },
});
Agent.make<unknown>()("NoUses", {
  tasks: {
    // @ts-expect-error Agent.use(Echo) without `uses: [Echo]`
    t: Agent.task({ input: Schema.Struct({}) }, () =>
      Effect.map(Agent.use({} as typeof Echo), () => null),
    ),
  },
});
