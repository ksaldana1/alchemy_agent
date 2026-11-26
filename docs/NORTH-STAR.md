# North star: the newsroom

> **Status (2026-11-25):** aspirational. None of this API exists yet. It shows where the
> milestones in [PLAN.md](PLAN.md) should lead, and it is the yardstick for API decisions. When a
> milestone's API drifts from this, either fix the API or update this doc on purpose and say why.

This is the newsroom from `mini-agent-runtime/examples/newsroom/`, written the way it should read
once M2–M8 exist. It is the M8 acceptance demo:

1. A submission lands in the shared area, which starts a workflow run.
2. Intake files the article on the editors' desk.
3. The editor, an LLM agent, edits it and shares it with the senior reviewer.
4. The junior reviewer is denied, so the senior reviewer takes over.
5. A person signs off on that exact revision, and the article is published.

Terms follow [GLOSSARY.md](GLOSSARY.md).

## `src/agents.ts`

```ts
import { Agent, AgentModel, Workspace } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/** Intake: files a submission on the editors' desk, and later publishes it. */
export class Intake extends Agent.make<Intake>()("Intake", {
  file: Agent.task({ from: Schema.String, to: Schema.String }, ({ from, to }) =>
    Workspace.read(from).pipe(Effect.flatMap((text) => Workspace.write(to, text.toUpperCase()))),
  ),
  publish: Agent.task({ from: Schema.String, name: Schema.String }, ({ from, name }) =>
    Workspace.copy(from, `shared/published/${name}.md`),
  ),
}) {}

/** Editor: an LLM agent that edits with permission-checked file tools. */
export class Editor extends Agent.make<Editor>()("Editor", {
  model: AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast"),
  instructions: "You are a copy editor. Fix grammar, tighten prose, never change facts.",
  tools: Workspace.toolkit, // read / write / list, checked against grants
  tasks: {
    // One durable turn: every model call and tool call is a checkpointed step.
    edit: Agent.task({ path: Schema.String, name: Schema.String }, ({ path, name }) =>
      Agent.prompt(`Edit ${path} and save it to ~/edited/${name}.md.`, { output: Workspace.FileRef }),
    ),
    share: Agent.task({ path: Schema.String, with: Agent.Address }, ({ path, with: to }) =>
      Workspace.share(path, to, "read"),
    ),
  },
}) {}

const Verdict = Schema.Struct({ approved: Schema.Boolean, notes: Schema.String });

/** Reviewer: reads the edited article and gives a structured verdict. */
export class Reviewer extends Agent.make<Reviewer>()("Reviewer", {
  model: AgentModel.gateway({ provider: "anthropic", model: "claude-sonnet-5-5" }),
  instructions: "You review edited articles for accuracy and tone.",
  tasks: {
    review: Agent.task({ path: Schema.String, name: Schema.String }, ({ path, name }) =>
      Effect.gen(function* () {
        // Without a grant this fails with FsDeniedError, a permanent failure.
        const text = yield* Workspace.read(path);
        const verdict = yield* Agent.prompt(`Review this article:\n\n${text}`, { output: Verdict });
        const { agent } = yield* Agent.AgentContext;
        return { ...verdict, by: agent.name, path, revision: yield* Workspace.revision(path) };
      }),
    ),
  },
}) {}
```

## `src/publish.ts`

```ts
import { AgentWorkflow, Human } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Editor, Intake, Reviewer } from "./agents.ts";

export class Rejected extends Schema.TaggedError<Rejected>()("Rejected", {
  by: Schema.String,
  notes: Schema.String,
}) {}

export const Publish = AgentWorkflow.make("Publish", {
  input: { path: Schema.String, name: Schema.String },
  run: ({ path, name }) =>
    Effect.gen(function* () {
      const intake = Intake.named("intake");
      const desk = Editor.named("desk");

      // Each call is a durable step: a task id derived from the workflow run, and the run
      // waits without holding execution. Calling an agent looks like calling a function.
      const filed = yield* intake.file({ from: path, to: `shared/desk/${name}.md` });
      const edited = yield* desk.edit({ path: filed.path, name });
      yield* desk.share({ path: edited.path, with: Reviewer.named("senior") });

      // The junior reviewer was never given access, so its review fails and the senior takes over.
      const review = yield* Reviewer.named("junior").review({ path: edited.path, name }).pipe(
        Effect.catchTag("TaskFailed", () => Reviewer.named("senior").review({ path: edited.path, name })),
      );
      if (!review.approved) return yield* new Rejected({ by: review.by, notes: review.notes });

      // A person signs off on exactly this revision. A sign-off for an older revision is rejected.
      yield* Human.approval("sign-off", { artifact: review.path, revision: review.revision, timeout: "2 days" });

      return yield* intake.publish({ from: review.path, name });
    }),
});
```

## `alchemy.run.ts`

```ts
import { Grant, Listen, Platform } from "alchemy-agent";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import { Editor, Intake, Reviewer } from "./src/agents.ts";
import { Publish } from "./src/publish.ts";

export const Newsroom = Platform.make("Newsroom", {
  agents: [Intake, Editor, Reviewer],
  workflows: [Publish],
  // Only the platform grants access to the shared area. The editor shares its own files per article.
  grants: [
    Grant.read(Intake.named("intake"), "shared/submissions/"),
    Grant.write(Intake.named("intake"), "shared/desk/"),
    Grant.write(Intake.named("intake"), "shared/published/"),
    Grant.read(Editor.named("desk"), "shared/desk/"),
  ],
  // Exactly one run per uploaded file, even if the storage event is delivered twice.
  listeners: [
    Listen.files("shared/submissions/", { suffix: ".txt" }).startWorkflow(Publish, (file) => ({
      path: file.path,
      name: file.basename,
    })),
  ],
});

export default Alchemy.Stack(
  "Newsroom",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    // Provisions storage, the grants database, a queue and dead-letter queue,
    // the API Worker, and one Durable Object class per agent.
    const newsroom = yield* Newsroom;
    return { url: newsroom.url };
  }),
);
```

## `scripts/demo.ts` (replaces the reference's `main.ts`)

```ts
const program = Effect.gen(function* () {
  const newsroom = yield* Platform.Client;
  for (const [name, text] of Object.entries(ARTICLES)) {
    yield* newsroom.files.write(`shared/submissions/${name}.txt`, text);
  }
  // Play the person: approve each sign-off request as it appears.
  yield* newsroom.requests.stream.pipe(
    Stream.take(3),
    Stream.runForEach((request) => request.respond({ approved: true })),
    Effect.forkScoped,
  );
  const runs = yield* newsroom.runs(Publish).pipe(Stream.take(3), Stream.runCollect);
  for (const run of runs) {
    const published = yield* run.result();
    console.log(run.input.name, run.step("edit").attempts, run.step("review").agent, published.path);
  }
}).pipe(Effect.provide(Platform.client({ url, token })), Effect.scoped);
```

## `test/newsroom.test.ts` (chaos lives in tests, never in app code)

```ts
test("every step takes effect once under chaos", Effect.gen(function* () {
  yield* Chaos.crashOnce(Editor, "edit");                   // the editor dies mid-edit
  yield* Chaos.redeployDuring(Publish, "sign-off");         // a deploy lands while a run waits on a person
  yield* Chaos.duplicateEvents(Newsroom.listener("submissions"));

  const runs = yield* publishAll(ARTICLES);
  expect(runs).toHaveLength(3);                                   // duplicate events start no extra runs
  expect(runs.map((r) => r.step("edit").attempts).sort()).toEqual([1, 1, 2]);
  expect(runs.every((r) => r.step("review").agent === "senior")).toBe(true);
}));
```

## The design ideas this shows

- **An agent reference is a typed value, and calling it looks the same everywhere.**
  `Reviewer.named("junior").review(...)` reads the same in a workflow, in another agent's handler,
  and in a script. Only the context changes how the call waits:
  - In a workflow, it's a durable step with a task id derived from the run.
  - In a handler, the task goes into a waiting state and holds no execution (M3).
  - In a script, it goes over HTTP.
- **Agent and workflow code contain no infrastructure.** No buckets, queues, bindings, or retry
  loops appear there. Grants and listeners are declared together in one `Platform.make`.
- **Failure handling is plain Effect.** A permission denial is a typed, permanent `TaskFailed`, so
  the fallback reviewer is a `catchTag`. Crashes, redeploys, and duplicate events are the
  platform's problem, and they show up only in tests.
- **Definitions stay light.** Task inputs are struct fields, and options appear only when needed.
  Types flow from the definition to every call site.
- **The bar from M8:** the app code (agents, workflow, and platform) is about the size of the
  reference's agents plus its wiring, while adding an LLM editor and reviewer, a person's sign-off,
  and publishing.

## How far today's code is from this

M1's API, as of 2026-11-25, uses `send("task", input)` with string task names, `TaskHandle`s that
poll, `uses: [X]` plus `Agent.use(X)` to reach other agents, and an `Api.worker` that owns its
Worker. The planned API pass closes that gap before M2:
- method-style calls that wait by default, with an explicit `send` for fire-and-forget
- struct-field task inputs, and tasks with no input
- a mountable API instead of one that owns the Worker
- testing hooks moved out of the public module

## What is invented beyond the plan

- **The unified `Agent.named(...)` call.** It needs a context-provided "how to reach agents"
  service with three implementations: workflow, handler, and HTTP.
- **`Listen.files`** over the workspace instead of the plan's `Listen.r2(bucket)`, which names a
  bucket and so breaks principle 10.
- **Static `grants` in `Platform.make`**, seeded at deploy.
- **`Human.approval`, `Workspace.revision`, `Workspace.copy`, and `Chaos.*`.**

## What it depends on

- `Agent.prompt` with a structured `output` assumes spike 0.5 shows Workers AI (and the gateway)
  can produce structured output, with every step checkpointed.
- `Publish` assumes either engine from spike 0.4 can derive task ids from the run, replay
  `catchTag` deterministically (each agent call must be a recorded step), and resume after a
  redeploy.
