# Library guide

Start with the [README](../README.md) to run an example and define your first agent.
This guide covers the API in more detail. Snippets build on the contracts and services named
in each section; the [examples](#examples) contain complete applications.

- [Agents](#agents)
- [Calling agents](#calling-agents)
- [LLM agents](#llm-agents)
- [The workspace](#the-workspace)
- [Workflows](#workflows)
- [Human requests](#human-requests)
- [Listeners](#listeners)
- [The platform and its HTTP API](#the-platform-and-its-http-api)
- [From scripts](#from-scripts)
- [Testing](#testing)
- [Rules to know](#rules-to-know)
- [Examples](#examples)

## Agents

An **agent definition** is `Agent.make(type, rpcs)`. An **agent** is one name within it:
`Greeter/ada` and `Greeter/bob` are two agents, each with its own inbox and history. An agent
exists as soon as something addresses it.

**Handlers** get the decoded payload, plus where and how they're running:

```ts
Greeter.of({
  Greet: ({ payload, agent, taskId, attempt }) => …,
})
```

`toLayer` also takes a construction Effect, for when handlers need other clients or services:

```ts
export const GreeterLive = Greeter.toLayer(
  Effect.gen(function* () {
    const greeters = yield* Greeter.client;
    return Greeter.of({
      Greet: ({ payload }) => Effect.succeed({ greeting: `Hello, ${payload.name}!` }),
      // Introduce { name, via } is another Rpc in Greeter's contract: it asks another greeter.
      Introduce: ({ payload }) => greeters(payload.via).Greet({ name: payload.name }),
    });
  }),
  { retry: { maxAttempts: 5, delay: "2 seconds" } },
);
```

Whatever the handlers need beyond what the library provides becomes a requirement of the Layer,
and you supply it with `Layer.provide`, as in any Effect program.

**Errors are part of the contract.** A handler that fails with its Rpc's declared `error` settles
the task right away, and callers get that error as itself. Any other failure, or a defect, is
retried up to the attempt cap.

```ts
export class OutOfStock extends Schema.TaggedError<OutOfStock>()("OutOfStock", { sku: Schema.String }) {}

export const Reserve = Rpc.make("Reserve", {
  payload: { sku: Schema.String },
  success: Schema.Struct({ reservation: Schema.String }),
  error: OutOfStock, // fails once, reaches the caller as OutOfStock
});
```

**Idempotency comes from the contract.** With a `primaryKey`, the task id is `<tag>:<key>`, so
sending the same key twice records one task. A settled task never runs again, and its outcome
never changes. The key stands for the payload: the same key with a different payload (key order
aside) is refused with `AgentError(InputConflict)`, and nothing is recorded.

```ts
export const Welcome = Rpc.make("Welcome", {
  payload: { key: Schema.String, name: Schema.String },
  success: Schema.Struct({ greeting: Schema.String }),
  primaryKey: ({ key }) => key,
});
```

**Retries.** The defaults are 3 attempts, a 1 s delay that doubles, and a 5 min cap. Override
them per agent (`toLayer(…, { retry })`) or per task with an annotation, either with that
shorthand or with any Effect `Schedule`, as you would give `Effect.retry`:

```ts
Rpc.make("Sync", { … }).annotate(Agent.Retry, { delay: "30 seconds", maxAttempts: 10 })
Rpc.make("Fetch", { … }).annotate(Agent.Retry, Schedule.exponential("1 second").pipe(Schedule.jittered, Schedule.upTo({ times: 5 })))
```

A schedule is stepped once per failure, with the failure as its input, and the task fails with
`RetriesExhausted` when it ends. Retries outlive executions, so no schedule state is kept: each
failure replays the schedule from its start on a clock that advances by its own delays. A
schedule that never ends stops after 100 attempts.

**Cancellation.** `Agent.cancel(address, taskId, { reason })` settles a task that hasn't settled
as `AgentError(Canceled)`, which is what anyone waiting for it gets. A running handler is
interrupted and nothing it does afterwards is recorded; its LLM turns end; its calls that weren't
delivered yet are dropped. Tasks it already sent to other agents keep running. It succeeds with
`false` if the task had already settled.

```ts
yield* Agent.cancel(Reviewer.address("senior"), "Review:draft-42", { reason: "withdrawn" });
```

**Retention.** Settled tasks are kept forever by default. `toLayer(…, { retention: "30 days" })`
prunes them (with their turns, their calls, and the transcripts of sessions they had to themselves)
while the agent has work. A pruned task id is free again: sending it records a new task, so keep
the retention longer than anyone might resend.

---

## Calling agents

A client calls an agent by name. Each task is a method, typed from its Rpc:

```ts
const greeters = yield* Greeter.client;
const { greeting } = yield* greeters("ada").Greet({ name: "Ada" }); // waits for the result
yield* greeters("ada").Welcome({ key: "k1", name: "Ada" }, { discard: true }); // only sends
```

The same code works everywhere. What the context changes is *how* it reaches the agent:

| Where the call runs | How it reaches the agent | How it waits |
|---|---|---|
| A handler or a tool | Through the agent's outbox, sent once | The task (or turn) is `waiting` and holds no execution |
| A workflow | A durable step, sent once | The run waits without holding execution |
| A Worker you host | The Durable Object binding | The caller waits for the result |
| A script or test | HTTP (`Platform.connect` / `Api.connect`) | By polling the API |

**Failures.** A declared error arrives as itself, so `catchTag` works across agents. Everything
the platform can't deliver is one `AgentError` with a tagged `reason`:

```ts
yield* reviewers("junior").Review({ path }).pipe(
  Effect.catchTag("FsDenied", () => reviewers("senior").Review({ path })), // declared by Review
  Effect.catchTag("AgentError", (e) => …), // e.reason: Unreachable | RetriesExhausted | TaskNotFound | Canceled | InputConflict | …
);
```

---

## LLM agents

`Agent.generate` runs one durable LLM turn inside a handler:

```ts
import { Agent, AgentModel } from "alchemy-agent";

export const Verdict = Schema.Struct({ approved: Schema.Boolean, notes: Schema.String });

export const ReviewerLive = Reviewer.toLayer(
  Reviewer.of({
    Review: ({ payload }) =>
      Agent.generate({
        system: "You review articles for accuracy and tone.",
        prompt: payload.text,
        schema: Verdict, // omit for plain text
      }),
  }),
).pipe(Layer.provide(AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast")));
```

- **Answers.** Without a `schema` the answer is the model's text. With one, the answer is decoded
  into the schema's type. A schema whose encoded form is a string is answered in text. Any other
  schema is answered through a `respond` tool, and an answer that doesn't decode goes back to the
  model within the same turn.
- **Durability.** Every model call and every tool call is a recorded step. A crash or redeploy
  mid-turn resumes at the last step, and a recorded tool call never runs twice.
- **Sessions.** `session: "chat-42"` continues a conversation. Without it, each call is a new
  session. Turns in one session run one at a time.
- **Models are Layers:**
  - `AgentModel.workersAI(model)`, with no API keys.
  - `AgentModel.gateway(gateway, model)`, for Workers AI through an AI Gateway.
  - `AgentModel.scripted(...)`, for tests.
  - Any Effect AI `LanguageModel` Layer.
- **Limits:** `toLayer(…, { limits: { maxSteps, maxTokens, stepTimeout } })`. The defaults are 24
  steps per turn, no token cap, and 60 s per model call. Hitting a limit ends the turn with a
  `GenerationError`.

**Tools** are an Effect AI `Toolkit` declared on the agent, with handlers provided as a Layer. A
tool can call other agents and wait for them like any other code. `Agent.ToolCall` gives a
stable `idempotencyKey` for side effects outside the platform.

```ts
const LookupOrder = Tool.make("lookup_order", {
  description: "Look up an order by id.",
  parameters: Schema.Struct({ orderId: Schema.String }),
  success: Schema.Struct({ status: Schema.String }),
});
const Tools = Toolkit.make(LookupOrder);
const ToolsLive = Tools.toLayer(Tools.of({ lookup_order: ({ orderId }) => orders.status(orderId) }));

export const HelpdeskLive = Helpdesk.toLayer(
  Helpdesk.of({ Chat: ({ payload }) => Agent.generate({ prompt: payload.message, session: payload.session }) }),
  { toolkit: Tools },
).pipe(Layer.provide([Model, ToolsLive]));
```

**Watching a session.** `GET /agents/:type/:name/sessions/:id` returns the transcript, and
`…/events` streams it as Server-Sent Events. Reconnect with `?after=<seq>` to resume.

> A handler that calls `Agent.generate` waits while the turn runs, then **runs again from the
> top**. The turn returns its stored answer the second time, so code before it can run more than
> once. See [Rules to know](#rules-to-know).

---

## The workspace

The workspace holds files for agents. Paths are logical:

| Path | What it is |
|---|---|
| `~/notes.md` | The calling agent's home. Its owner always has full access. |
| `agents/Reviewer/senior/notes.md` | Another agent's home. Needs a share. |
| `shared/…` | The shared area. Only the platform grants access to it. |

Inside a handler or a tool, the caller is always the running agent. It's never an argument:

```ts
import { Workspace } from "alchemy-agent";

const text = yield* Workspace.read("shared/desk/article.md");
const ref = yield* Workspace.write("~/edited/article.md", text.trim()); // { path, revision, size }
yield* Workspace.share(ref.path, Reviewer.address("senior"), "read");
```

Operations: `read`, `write`, `list`, `stat`, `revision`, `copy`, `share`, `revoke`.

- **Grants.** The most specific grant covering a path decides. A grant on `shared/desk` covers
  `shared/desk/…` but not `shared/desk2`. Only an owner can share, and only from its own home, so
  a share can't be passed on.
- **Denials** fail with `Workspace.FsDenied` and are never retried. Declare `FsDenied` on an Rpc
  and callers can `catchTag` it. Otherwise the task fails with `AgentError(reason: FsDenied)`.
- **The audit.** Every allow and every deny is recorded. You can read it at `GET /workspace/audit`.
- **Revisions.** `revision` is the file's version: every write gets a new one. Approvals refer to
  revisions (see [Human requests](#human-requests)).

Platform grants are declared on the platform:

```ts
Platform.make("Newsroom", {
  …,
  grants: [
    Grant.read(Intake.address("intake"), "shared/submissions/"),
    Grant.write(Intake.address("intake"), "shared/desk/"),
  ],
});
```

**LLM agents** can use the workspace as tools: `{ toolkit: Workspace.Toolkit }` plus
`Layer.provide(Workspace.ToolkitLayer)` gives the model `read_file`, `write_file`, `list_files`,
and `share_file`. A denied tool call comes back to the model as a typed failure, and the turn
carries on.

---

## Workflows

Workflows are plain [`effect/workflow`](https://effect.website) code, hosted by the platform. Each
agent call in a workflow is a durable step: it is sent once, and the run waits for the answer
without holding any execution. The run survives crashes and redeploys.

```ts
import { Signal } from "alchemy-agent";
import * as Workflows from "alchemy/Workflows";
import { DurableClock, Workflow } from "effect/workflow";

export const Publish = Workflow.make("Publish", {
  payload: { path: Schema.String, name: Schema.String },
  success: Workspace.FileRef,
  error: Rejected,
  idempotencyKey: ({ path }) => path, // the same key is the same run
});

export const Approve = Signal.make("approve", Schema.Struct({ approved: Schema.Boolean }));

export const PublishLive = Workflows.layer(
  Publish,
  Effect.fn(function* ({ path, name }) {
    const desk = (yield* Editor.client)("desk");
    const reviewers = yield* Reviewer.client;

    const edited = yield* desk.Edit({ path, name });
    const [a, b] = yield* Effect.all(
      [reviewers("junior").Review({ path: edited.path }), reviewers("senior").Review({ path: edited.path })],
      { concurrency: "unbounded" },
    );
    yield* DurableClock.sleep({ name: "cool-off", duration: "1 hour" });

    const ok = yield* Signal.await(Approve, { timeout: "2 days" }).pipe(
      Effect.catchTag("SignalTimeout", () => Effect.succeed({ approved: false })),
    );
    if (!ok.approved) return yield* new Rejected({ by: "editor-in-chief" });
    return yield* desk.Publish({ path: edited.path, name });
  }),
);

// Platform.make("Newsroom", { …, workflows: [PublishLive] })
```

- **Starting runs:** from a listener (see [Listeners](#listeners)), over HTTP
  (`POST /workflows/Publish/runs { payload }`), or from a script (`client.start(Publish, payload)`).
- **Signals.** `Signal.make(name, schema)` defines a signal, and `Signal.await` waits for it. A
  timeout is durable and fails with `SignalTimeout`. Send a value with
  `POST /workflows/:name/runs/:id/signals/:signal { value }`. The first value counts. A signal sent
  before the run waits is kept, and one sent after the timeout changes nothing.
- **Status:** `GET /workflows/:name/runs/:id` → `Running`, `Completed` (with the result),
  `Failed` (with the declared error), or `Died`.

Workflows run on Alchemy's Durable Object engine, which `Platform.make` sets up for you.

---

## Human requests

A human request is a durable question for a person. It has a typed answer, an expiry, and
optionally the file revision it is about. It works in workflows, handlers, and tools, and the
caller waits without holding any execution.

```ts
import { Human } from "alchemy-agent";

// Approve exactly this revision. A no fails with NotApproved.
yield* Human.approval("sign-off", { artifact: review.path, revision: review.revision, timeout: "2 days" });

// Any typed question.
const { amount } = yield* Human.request("budget", {
  schema: Schema.Struct({ amount: Schema.Number }),
  prompt: "What's the budget for Q3?",
  timeout: "1 day",
});
```

It ends one of three ways:

- **Resolved:** the person answered.
- **Expired:** the timeout passed, and the caller gets `RequestExpired`.
- **Canceled:** someone canceled it, and the caller gets `RequestCanceled`.

People answer over HTTP:

```sh
curl "$URL/requests" -H "authorization: Bearer $TOKEN"   # pending requests: prompt, JSON Schema, artifact
curl -X POST "$URL/requests/<id>/respond" -H "authorization: Bearer $TOKEN" \
  -d '{ "response": { "approved": true }, "by": "editor-in-chief", "revision": "<revision>" }'
```

- **Validation.** A response that doesn't match the request's schema is a `400`.
- **First answer wins.** A second answer is a no-op (`409 already settled`).
- **Revisions.** An answer about a file must name the request's revision, and that must still be
  the file's current revision. Otherwise the answer is stale and rejected (`409`), and the request
  stays pending.

---

## Listeners

Listeners turn events into work, even while every agent is idle:

```ts
import { Listen } from "alchemy-agent";

Platform.make("Newsroom", {
  …,
  listeners: [
    // A run per uploaded file in the shared area.
    Listen.files("shared/submissions/", { suffix: ".txt" }).start(Publish, (file) => ({ path: file.path, name: file.basename })),
    // A task per cron fire (UTC).
    Listen.cron("0 9 * * *").send(Digest, "daily", "Summarize", (fire) => ({ day: new Date(fire.scheduledTime).toISOString() })),
  ],
});
```

- **What an event carries.** A file event has `path`, `basename`, `revision`, `size`, and
  `eventTime`. A cron fire has `cron` and `scheduledTime`.
- **Exactly once comes from the target.** Events are delivered at least once, so make the
  target idempotent:
  - a workflow's `idempotencyKey`. A key of `path` gives one run per file; `path` plus
    `revision` gives one per version.
  - an Rpc's `primaryKey`. Without one, the task id is `listener/<name>/<event id>`.
  A duplicate or replayed event then creates nothing new.
- **Failures.** A mapping that throws, or a payload that doesn't decode, is retried 3 times. Then
  it is dead-lettered, logged as `listener.error`, and recorded.
- **Inspecting and replaying.** `GET /listeners/log` shows every delivery, with the run or task it
  created. `POST /listeners/events { message }` replays an event.
- **Locally**, R2 sends no notifications. Put events on the queue with `POST /listeners/events`.

---

## The platform and its HTTP API

`Platform.make(name, { main, agents, workflows?, grants?, listeners?, workspace? })` builds one
Worker. It hosts every agent's Durable Object, the workflow engine, the workspace (when you
declare grants or file listeners), the listeners' queues, and the HTTP API. If you write your own
Worker, `Platform.layer({ … })` is the same thing as a Layer, and `Api.routes()` mounts the API.

Every route needs `Authorization: Bearer <AGENT_API_TOKEN>`. The API is an Effect `HttpApi`,
exported as `Api.Definition`: build an OpenAPI document from it (`OpenApi.fromApi`), or a typed
client of your own (`HttpApiClient.make(Api.Definition, …)`), which is what `Api.connect` and
`Platform.connect` use. Errors are JSON with an `error` field and a `_tag`.

| Route | What it does |
|---|---|
| `POST /agents/:type/:name/tasks` `{ tag, payload, taskId? }` | Creates a task (`202`, the task record). `409` if the task id is taken by another task or payload. |
| `GET /agents/:type/:name/tasks/:taskId[?wait=1]` | Returns a task. `wait=1` holds the request until it settles (up to 20 s). |
| `POST /agents/:type/:name/tasks/:taskId/cancel` `{ reason? }` | Cancels a task (`200`, the record; `409` if it had already settled) |
| `GET /agents/:type/:name/tasks[?status=…]` | The agent's inbox |
| `GET /agents[?type=…]` | Agents that have had work |
| `GET /agents/:type/:name/sessions/:id[/events]` | A session's transcript, or its live SSE stream |
| `POST /workflows/:name/runs` `{ payload }` | Starts a run (the same key is the same run) |
| `GET /workflows/:name/runs/:id` | A run's status |
| `POST /workflows/:name/runs/:id/signals/:signal` `{ value }` | Signals a run |
| `GET /requests[?status=pending\|resolved\|expired\|canceled\|all]` | Human requests |
| `POST /requests/:id/respond` `{ response, by, revision? }` | Answers a request |
| `POST /requests/:id/cancel` `{ by, reason? }` | Cancels a request |
| `GET\|PUT /workspace/files/<path>` | Reads or writes a file, as the platform |
| `GET /workspace/list/<path>`, `GET /workspace/audit` | Lists a directory; reads the audit |
| `GET /listeners/log`, `POST /listeners/events` `{ message }` | Listener deliveries; replays an event |

---

## From scripts

`Platform.connect` gives scripts and tests a typed client. Agent clients go over HTTP too:

```ts
import { Platform } from "alchemy-agent";
import * as FetchHttpClient from "effect/http/FetchHttpClient";

const program = Effect.gen(function* () {
  const newsroom = yield* Platform.Client;

  // Approve each sign-off as it appears.
  yield* newsroom.requests.stream.pipe(
    Stream.runForEach((request) => request.respond({ approved: true }, { by: "editor-in-chief" })),
    Effect.forkScoped,
  );

  yield* newsroom.files.write("shared/submissions/bike-lanes.txt", article);
  const [run] = yield* newsroom.runs(Publish).pipe(Stream.take(1), Stream.runCollect);
  const published = yield* run.result; // typed by the workflow

  // Agent clients work here as well.
  const verdict = yield* (yield* Reviewer.client)("senior").Review({ path: published.path });
});

await Effect.runPromise(
  program.pipe(Effect.provide(Platform.connect({ url, token })), Effect.provide(FetchHttpClient.layer), Effect.scoped),
);
```

`Platform.Client` has:

- `files.read` / `files.write`
- `requests.pending` / `requests.stream`, each request with a `respond` method
- `start(workflow, payload)` and `run(workflow, id)`
- `runs(workflow)`, which streams the runs listeners start
- `listeners.replay`

A run has `status`, a typed `result`, and `tasksOn(agent)`.

---

## Testing

Tests run against a real stack: locally on workerd, or deployed to a test stage. Use
[`@alchemy.run/test`](https://alchemy.run) with `ALCHEMY_TEST_DEV=1` to run locally:

```ts
import * as Test from "@alchemy.run/test/Bun";

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({ providers: Cloudflare.providers(), state: Cloudflare.state() });
const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll(destroy(Stack));

test("greets", Effect.gen(function* () {
  const { url } = yield* stack;
  const greeting = yield* Effect.flatMap(Greeter.client, (g) => g("ada").Greet({ name: "Ada" })).pipe(
    Effect.provide(Platform.connect({ url: url!, token })),
  );
  expect(greeting.greeting).toBe("Hello, Ada!");
}));
```

- **A scripted model** makes LLM agents deterministic. Return replies from a list, or from a
  function of the prompt:
  `AgentModel.scripted((prompt) => AgentModel.toolCall("read_file", { path }))`. The helpers are
  `text`, `toolCall`, `respond`, and `empty`.
- **Chaos** (`alchemy-agent/testing`): a test platform can make the first delivery of a task
  crash, the way an eviction would. The task is then delivered again.

  ```ts
  import * as Testing from "alchemy-agent/testing";

  Platform.make("NewsroomTest", { …, provide: Testing.chaos([Testing.crashOnce(Editor, "Edit")]) });
  ```

  Inside handlers and tools, `Testing.crashExecution` crashes on the spot, and
  `Testing.count(name)` is a durable counter for asserting how many times something really ran.

---

## Rules to know

- **Handlers can run more than once.** Delivery is at least once, and a handler runs again
  whenever its task resumes: after an agent call or `Agent.generate` it waited on, or after a
  person's answer. Recorded calls reuse their results, but handler code and external side effects
  can repeat. For outside side effects, use the task id (or `Agent.ToolCall`'s `idempotencyKey`)
  as the idempotency key.
- **Make the same calls in the same order.** Calls inside a handler, a tool, or a workflow are
  identified by their position. A run that makes a different call at the same position stops
  with an error instead of getting another call's answer. Don't branch on time or randomness
  before a call.
- **Waiting interrupts the handler.** A client call, `Agent.generate`, or `Human.*` that has to
  wait suspends the execution by interrupting it, as `effect/workflow` does, so `catch`,
  `catchTag`, `catchDefect`, `orElse`, and `retry` never see it. Once an execution has suspended,
  its outcome is discarded: code that catches the interruption (`catchCause`, `exit`) runs on for
  nothing, and its side effects can repeat. Keep those away from agent calls.
- **Don't call agents inside an `Activity`.** Call them from the workflow body, where each call
  is its own step.
- **Redeploys are gradual.** A Durable Object can run the previous code for a few minutes after
  a deploy. Add tasks and fields; don't remove or change them while work is in flight.
- **Deployed Workers freeze the clock during synchronous code.** Never write busy loops or
  timeouts that rely on `Date.now()` advancing.

---

## Examples

| Example | Shows |
|---|---|
| [`examples/hello`](../examples/hello) | Agents, retries, keyed tasks, agents waiting on agents |
| [`examples/support-desk`](../examples/support-desk) | LLM agents on Workers AI, tools that call other agents, the workspace |
| [`examples/newsroom`](../examples/newsroom) | Everything together: listener → workflow → LLM editor and reviewers → human sign-off → publish, plus an acceptance test under chaos |

The newsroom's [`test/newsroom.test.ts`](../examples/newsroom/test/newsroom.test.ts) and
[`scripts/newsroom.ts`](../examples/newsroom/scripts/newsroom.ts) are good places to start reading.
[`docs/NORTH-STAR.md`](NORTH-STAR.md) walks through the newsroom's design.
