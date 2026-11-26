# North star: the newsroom

> **Status (2026-11-26):** aspirational, rewritten in Effect's own idiom (the second version; the
> first is in git history). It shows where the milestones in [PLAN.md](PLAN.md) should lead, and
> it is the yardstick for API decisions. When a milestone's API drifts from this, either fix the
> API or update this doc on purpose and say why.

This is the newsroom from `mini-agent-runtime/examples/newsroom/`, written the way it should read
once M2–M8 exist. It is the M8 acceptance demo:

1. A submission lands in the shared area, which starts a workflow run.
2. Intake files the article on the editors' desk.
3. The editor, an LLM agent, edits it and shares it with the senior reviewer.
4. The junior reviewer is denied, so the senior reviewer takes over.
5. A person signs off on that exact revision, and the article is published.

Terms follow [GLOSSARY.md](GLOSSARY.md). The API mirrors Effect's own idioms (see
`repos/effect/LLMS.md` and `repos/effect/ai-docs/src`):
- an agent is like an `effect/cluster` `Entity`, and its tasks are `effect/rpc` `Rpc`s
- implementations are Layers built with `toLayer` and `of`
- tools are Effect AI `Toolkit`s with `toLayer` handlers
- workflows are `effect/workflow` `Workflow`s
- errors are `Schema.TaggedError`s

## `src/contracts.ts`: what the agents accept and return

Contracts hold no runtime code. Any module (another agent, a workflow, a script) can import them.

```ts
import { Agent, Workspace } from "alchemy-agent";
import { Rpc } from "effect/rpc";
import * as Schema from "effect/Schema";

// Intake: files submissions on the desk, and publishes approved articles.
export const File = Rpc.make("File", {
  payload: { from: Schema.String, to: Schema.String },
  success: Workspace.FileRef,
  error: Workspace.FsDenied,
});
export const Publish = Rpc.make("Publish", {
  payload: { from: Schema.String, name: Schema.String },
  success: Workspace.FileRef,
});
export const Intake = Agent.make("Intake", [File, Publish]);

// Editor: an LLM agent that edits, then shares the result.
export const Edit = Rpc.make("Edit", {
  payload: { path: Schema.String, name: Schema.String },
  success: Workspace.FileRef,
});
export const Share = Rpc.make("Share", {
  payload: { path: Schema.String, with: Agent.Address },
});
export const Editor = Agent.make("Editor", [Edit, Share]);

// Reviewer: an LLM agent with a structured verdict. A missing grant is a declared error.
export const Verdict = Schema.Struct({ approved: Schema.Boolean, notes: Schema.String });
export const Review = Rpc.make("Review", {
  payload: { path: Schema.String, name: Schema.String },
  success: Schema.Struct({ ...Verdict.fields, by: Schema.String, path: Schema.String, revision: Schema.String }),
  error: Workspace.FsDenied,
});
export const Reviewer = Agent.make("Reviewer", [Review]);
```

## `src/agents.ts`: how they do it

```ts
import { Agent, AgentModel, Workspace } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Editor, Intake, Reviewer, Verdict } from "./contracts.ts";

export const IntakeLive = Intake.toLayer(
  Effect.gen(function* () {
    return Intake.of({
      File: Effect.fn("Intake.File")(function* ({ payload: { from, to } }) {
        const text = yield* Workspace.read(from);
        return yield* Workspace.write(to, text.toUpperCase());
      }),
      Publish: ({ payload: { from, name } }) => Workspace.copy(from, `shared/published/${name}.md`),
    });
  }),
);

export const EditorLive = Editor.toLayer(
  Effect.gen(function* () {
    return Editor.of({
      // One durable LLM turn: every model call and tool call is a checkpointed step.
      Edit: ({ payload: { path, name } }) =>
        Agent.generate({
          system: "You are a copy editor. Fix grammar, tighten prose, never change facts.",
          prompt: `Edit ${path} and save it to ~/edited/${name}.md.`,
          toolkit: Workspace.Toolkit, // read / write / list, checked against grants
          schema: Workspace.FileRef,
        }),
      Share: ({ payload }) => Workspace.share(payload.path, payload.with, "read"),
    });
  }),
).pipe(Layer.provide([AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast"), Workspace.ToolkitLayer]));

export const ReviewerLive = Reviewer.toLayer(
  Effect.gen(function* () {
    return Reviewer.of({
      Review: Effect.fn("Reviewer.Review")(function* ({ payload: { path }, agent }) {
        // Without a grant this fails with FsDenied: a declared error, so the task settles.
        const text = yield* Workspace.read(path);
        const verdict = yield* Agent.generate({
          system: "You review edited articles for accuracy and tone.",
          prompt: `Review this article:\n\n${text}`,
          schema: Verdict,
        });
        return { ...verdict, by: agent.name, path, revision: yield* Workspace.revision(path) };
      }),
    });
  }),
).pipe(Layer.provide(AgentModel.gateway({ provider: "anthropic", model: "claude-sonnet-5-5" })));
```

## `src/publish.ts`: the workflow

```ts
import { Human, Workspace } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Workflow } from "effect/workflow";
import { Editor, Intake, Reviewer } from "./contracts.ts";

export class Rejected extends Schema.TaggedError<Rejected>()("Rejected", {
  by: Schema.String,
  notes: Schema.String,
}) {}

export const PublishArticle = Workflow.make({
  name: "PublishArticle",
  payload: { path: Schema.String, name: Schema.String },
  success: Workspace.FileRef,
  error: Rejected,
  idempotencyKey: ({ path }) => path,
});

export const PublishArticleLive = PublishArticle.toLayer(
  Effect.fn(function* ({ path, name }) {
    const intake = (yield* Intake.client)("intake");
    const desk = (yield* Editor.client)("desk");
    const reviewers = yield* Reviewer.client;

    // Inside a workflow, each agent call is a durable step: it runs once, and the run waits
    // for the answer without holding execution.
    const filed = yield* intake.File({ from: path, to: `shared/desk/${name}.md` });
    const edited = yield* desk.Edit({ path: filed.path, name });
    yield* desk.Share({ path: edited.path, with: Reviewer.address("senior") });

    // The junior reviewer was never given access: its declared FsDenied error comes back as
    // itself, and the senior reviewer takes over.
    const review = yield* reviewers("junior").Review({ path: edited.path, name }).pipe(
      Effect.catchTag("FsDenied", () => reviewers("senior").Review({ path: edited.path, name })),
    );
    if (!review.approved) return yield* new Rejected({ by: review.by, notes: review.notes });

    // A person signs off on exactly this revision. A sign-off for an older revision is rejected.
    yield* Human.approval("sign-off", { artifact: review.path, revision: review.revision, timeout: "2 days" });

    return yield* intake.Publish({ from: review.path, name });
  }),
);
```

## `alchemy.run.ts`

```ts
import { Grant, Listen, Platform } from "alchemy-agent";
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import { EditorLive, IntakeLive, ReviewerLive } from "./src/agents.ts";
import { Editor, Intake } from "./src/contracts.ts";
import { PublishArticle, PublishArticleLive } from "./src/publish.ts";

export const Newsroom = Platform.make("Newsroom", {
  agents: [IntakeLive, EditorLive, ReviewerLive],
  workflows: [PublishArticleLive],
  // Only the platform grants access to the shared area. The editor shares its own files per article.
  grants: [
    Grant.read(Intake.address("intake"), "shared/submissions/"),
    Grant.write(Intake.address("intake"), "shared/desk/"),
    Grant.write(Intake.address("intake"), "shared/published/"),
    Grant.read(Editor.address("desk"), "shared/desk/"),
  ],
  // Exactly one run per uploaded file, even if the storage event is delivered twice.
  listeners: [
    Listen.files("shared/submissions/", { suffix: ".txt" }).start(PublishArticle, (file) => ({
      path: file.path,
      name: file.basename,
    })),
  ],
});

export default Alchemy.Stack(
  "Newsroom",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    // Provisions storage, the grants database, a queue and dead-letter queue, the API Worker
    // (an HttpApi, with OpenAPI), and one Durable Object class per agent.
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
  const runs = yield* newsroom.runs(PublishArticle).pipe(Stream.take(3), Stream.runCollect);
  for (const run of runs) {
    const published = yield* run.result;
    console.log(run.payload.name, run.step("Edit").attempts, run.step("Review").agent, published.path);
  }
}).pipe(Effect.provide(Platform.connect({ url, token }, Newsroom)), Effect.scoped);
```

## `test/newsroom.test.ts` (chaos lives in tests, never in app code)

```ts
test("every step takes effect once under chaos", Effect.gen(function* () {
  yield* Chaos.crashOnce(Editor, "Edit");                          // the editor dies mid-edit
  yield* Chaos.redeployDuring(PublishArticle, "sign-off");         // a deploy lands while a run waits on a person
  yield* Chaos.duplicateEvents(Newsroom.listener("submissions"));

  const runs = yield* publishAll(ARTICLES);
  expect(runs).toHaveLength(3);                                    // duplicate events start no extra runs
  expect(runs.map((r) => r.step("Edit").attempts).sort()).toEqual([1, 1, 2]);
  expect(runs.every((r) => r.step("Review").agent === "senior")).toBe(true);
}));
```

## The design ideas this shows

- **Contracts are separate from implementations.** `Agent.make("Reviewer", [Review])` is a
  value built from `Rpc` schemas, like `Entity.make`. The implementation is a Layer
  (`Reviewer.toLayer(... Reviewer.of({ Review: ... }))`). Workflows, scripts and other agents
  import only contracts, so there's no runtime coupling and no import cycles.
- **Calling an agent is calling a client:** `(yield* Reviewer.client)("junior").Review(payload)`
  waits for the answer, and `{ discard: true }` sends without waiting, as with Effect's entity and
  RPC clients. The context decides how a call waits:
  - in a workflow, it's a durable step
  - in a handler, the task waits (M3)
  - in a script, it goes over HTTP
- **Errors are part of the contract.** A declared `error` (`FsDenied`) settles the task at once
  and reaches the caller as the real class, so the fallback reviewer is a plain
  `Effect.catchTag("FsDenied", …)`. Undeclared failures are retried. Platform failures come back
  as one `AgentError` with a tagged `reason`.
- **Idempotency comes from the contract**, through `primaryKey` (Rpc) or `idempotencyKey`
  (Workflow), not through options at each call site.
- **Models and tools are Layers.** `AgentModel.*` provides a `LanguageModel`, and tools are a
  `Toolkit` plus its `toLayer` handlers.
- **One `Agent.generate`, typed by its schema.** Without a `schema` it returns text (the schema
  defaults to `Schema.String`, as an Rpc's `success` defaults to `Schema.Void`). With one, the
  answer decodes to the schema's type:
  - a schema whose encoded form is a string: the model answers in text
  - anything else: the model answers through a `respond` tool built from the JSON Schema

  Each call is a durable, step-checkpointed turn. An answer that doesn't decode goes back to the
  model within the turn.
- **There's no infrastructure in agent or workflow code.** Grants and listeners are declared
  together in one `Platform.make`.
- **The bar from M8:** the app code (contracts, agents, workflow, and platform) stays about the
  size of the reference's agents plus its wiring, while adding an LLM editor and reviewer, a
  person's sign-off, and publishing.

## How far today's code is from this

As of 2026-11-26, the library still has the M1/M2 API: `Agent.make<Self>()(name, { tasks })` with
`Agent.task`, references (`X.named(n).task(...)`, `send.*`, `handle`), `uses`, `Agent.prompt`,
`Agent.tools`, and errors flattened to `{ _tag, message }`. The API reset (in progress) moves it
to the shape above: `Rpc` contracts, `toLayer` / `of`, `client`, declared errors, `primaryKey`,
`Agent.generate`, toolkit Layers, and `Schema.TaggedError` throughout.

Still to come after the reset: waiting inside a handler (M3), the workspace (M4), workflows (M5),
listeners (M6), human requests (M7), and `Platform.make`.

## What is invented beyond the plan

- **The client used everywhere.** `X.client` needs a context-provided "how to reach agents"
  service with three implementations: workflow, handler, and HTTP.
- **`Listen.files`** over the workspace, instead of the plan's `Listen.r2(bucket)`, which names a
  bucket and so breaks principle 10.
- **Static `grants` in `Platform.make`**, seeded at deploy.
- **`Human.approval`, `Workspace.revision`, `Workspace.copy`, `X.address(name)`, and `Chaos.*`.**

## What it depends on

- `Agent.generate` with a schema: spike 0.5 and M2 showed durable steps work, and that
  structured output works through a validated `respond` tool (`generateObject` doesn't work
  through Alchemy's Workers AI adapter). Code before the call in a handler can run again when the
  task resumes; M5's workflows make that explicit with `Activity`.
- `AgentModel.gateway({ provider: "anthropic", … })` needs a provider package and an API key.
- `PublishArticle` assumes the workflow engine chosen in spike 0.4 (`effect/workflow` on
  Alchemy's Durable Object engine is the natural fit for this API) can treat each agent call as a
  recorded step, replay `catchTag` deterministically, and resume after a redeploy.
