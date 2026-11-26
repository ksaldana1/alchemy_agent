# alchemy-agent

Persistent, cooperating agents on Cloudflare, written in [Effect](https://effect.website) and
deployed with [Alchemy](https://alchemy.run).

An agent is a long-lived identity, such as `Reviewer/senior`, with a durable inbox. You describe
what it does as typed tasks. The library hosts it on a Durable Object and delivers its work. It
retries failures and resumes after crashes and redeploys. You never touch Durable Objects,
queues, or buckets yourself.

```ts
const review = yield* (yield* Reviewer.client)("senior").Review({ path, name });
```

That one line can run in a script, in another agent, or in a workflow. Its type comes from the
`Reviewer` contract, it reaches the agent wherever it lives, and it waits for the answer without
holding any execution.

What you get:

- **Agents with durable inboxes.** Tasks are recorded once per task id and delivered at least
  once. They're retried with backoff, and each one ends with a result or a typed error.
- **LLM agents.** `Agent.generate` runs one model turn. Every model call and tool call is a
  checkpoint, so a crash or redeploy mid-turn resumes from the last step. Models and tools are
  Effect AI Layers: Workers AI out of the box, and a scripted model for tests.
- **Agents that call each other**, and wait for the answer without blocking anything. A waiting
  task holds no execution, and A → B → A doesn't deadlock.
- **A permissioned workspace.** Agents get files under their own `~/`, a shared area the platform
  grants access to, sharing between agents, and an audit of every access.
- **Workflows.** Plain `effect/workflow` code where each agent call is a durable step. Workflows
  also get timers and typed signals with timeouts.
- **Human requests.** A durable question for a person, with a typed answer and an expiry. It can
  be bound to the exact file revision being approved.
- **Listeners.** A file upload or a cron schedule starts a workflow or creates a task, exactly
  once per event.
- **An HTTP API** for all of the above, plus a typed client for scripts and tests.

> **Status: pre-release.** It isn't on npm yet, and the API may still change. It is built on
> pinned versions: `alchemy` and `@alchemy.run/cloudflare` `2.0.0-beta.87`, and `effect`
> `4.1.5`. It runs on Bun.

---

## Contents

- [Getting started](#getting-started)
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

---

## Getting started

You need a Cloudflare account set up for Alchemy (`alchemy profile`) and Bun. Until the package is
published, use it as a workspace package of this repository (as `examples/*` do). Your app's
`package.json`:

```json
{
  "type": "module",
  "dependencies": {
    "alchemy-agent": "workspace:*",
    "alchemy": "2.0.0-beta.87",
    "@alchemy.run/cloudflare": "2.0.0-beta.87",
    "@effect/platform-bun": "4.1.5",
    "effect": "4.1.5"
  }
}
```

An app has four parts: **contracts** (what agents accept and return), **implementations**
(Layers), a **platform** (one Worker that hosts it all), and an Alchemy **stack**.

**1. A contract.** Contracts are Effect `Rpc`s and hold no runtime code, so anything can import
them to call the agent.

```ts
// src/contracts.ts
import { Agent } from "alchemy-agent";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";

export const Greet = Rpc.make("Greet", {
  payload: { name: Schema.String },
  success: Schema.Struct({ greeting: Schema.String }),
});

export const Greeter = Agent.make("Greeter", [Greet]);
```

**2. An implementation**, as a Layer:

```ts
// src/agents.ts
import * as Effect from "effect/Effect";
import { Greeter } from "./contracts.ts";

export const GreeterLive = Greeter.toLayer(
  Greeter.of({
    Greet: ({ payload }) => Effect.succeed({ greeting: `Hello, ${payload.name}!` }),
  }),
);
```

**3. A platform.** It lives in its own module, which the Worker bundles (`main`):

```ts
// src/platform.ts
import { Platform } from "alchemy-agent";
import { GreeterLive } from "./agents.ts";

export default Platform.make("HelloApi", {
  main: import.meta.url,
  agents: [GreeterLive],
});
```

**4. A stack:**

```ts
// alchemy.run.ts
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import Hello from "./src/platform.ts";

export default Alchemy.Stack(
  "Hello",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const hello = yield* Hello;
    return { url: hello.url };
  }),
);
```

The API is protected by a bearer token. Put it in a `.env` next to `alchemy.run.ts`:

```sh
AGENT_API_TOKEN=some-long-random-string
```

Deploy, then send a task:

```sh
bun --bun alchemy deploy --stage dev --yes

curl -X POST "$URL/agents/Greeter/ada/tasks" \
  -H "authorization: Bearer $AGENT_API_TOKEN" \
  -d '{ "tag": "Greet", "payload": { "name": "Ada" } }'
# → 202 { "taskId": "…", "status": "pending", … }

curl "$URL/agents/Greeter/ada/tasks/<taskId>?wait=1" -H "authorization: Bearer $AGENT_API_TOKEN"
# → { "status": "completed", "result": { "greeting": "Hello, Ada!" }, … }
```

`bun --bun alchemy dev` runs the same stack locally on workerd.

---

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
never changes.

```ts
export const Welcome = Rpc.make("Welcome", {
  payload: { key: Schema.String, name: Schema.String },
  success: Schema.Struct({ greeting: Schema.String }),
  primaryKey: ({ key }) => key,
});
```

**Retries.** The defaults are 3 attempts, a 1 s delay that doubles, and a 5 min cap. Override
them per agent (`toLayer(…, { retry })`) or per task with an annotation:

```ts
Rpc.make("Sync", { … }).annotate(Agent.Retry, { delay: "30 seconds", maxAttempts: 10 })
```

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
| A Worker you host | The Durable Object binding | In the agent's Durable Object |
| A script or test | HTTP (`Platform.connect` / `Api.connect`) | By polling the API |

**Failures.** A declared error arrives as itself, so `catchTag` works across agents. Everything
the platform can't deliver is one `AgentError` with a tagged `reason`:

```ts
yield* reviewers("junior").Review({ path }).pipe(
  Effect.catchTag("FsDenied", () => reviewers("senior").Review({ path })), // declared by Review
  Effect.catchTag("AgentError", (e) => …), // e.reason: Unreachable | RetriesExhausted | InvalidPayload | …
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

Every route needs `Authorization: Bearer <AGENT_API_TOKEN>`.

| Route | What it does |
|---|---|
| `POST /agents/:type/:name/tasks` `{ tag, payload, taskId? }` | Creates a task (`202`, the task record) |
| `GET /agents/:type/:name/tasks/:taskId[?wait=1]` | Returns a task. `wait=1` holds the request until it settles (up to 20 s). |
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
  person's answer. The platform makes its own effects happen once, but your code before a wait
  can repeat. For outside side effects, use the task id (or `Agent.ToolCall`'s `idempotencyKey`)
  as the idempotency key.
- **Make the same calls in the same order.** Calls inside a handler, a tool, or a workflow are
  identified by their position. A run that makes a different call at the same position stops
  with an error instead of getting another call's answer. Don't branch on time or randomness
  before a call.
- **Don't catch defects** (`catchCause`, `catchDefect`, `exit`) around client calls,
  `Agent.generate`, or `Human.*`. Waiting is signaled as a defect that the runtime needs to see.
  Catching typed errors (`catchTag`) is fine.
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
| [`examples/hello`](examples/hello) | Agents, retries, keyed tasks, agents waiting on agents |
| [`examples/support-desk`](examples/support-desk) | LLM agents on Workers AI, tools that call other agents, the workspace |
| [`examples/newsroom`](examples/newsroom) | Everything together: listener → workflow → LLM editor and reviewers → human sign-off → publish, plus an acceptance test under chaos |

The newsroom's [`test/newsroom.test.ts`](examples/newsroom/test/newsroom.test.ts) and
[`scripts/newsroom.ts`](examples/newsroom/scripts/newsroom.ts) are good places to start reading.
[`docs/NORTH-STAR.md`](docs/NORTH-STAR.md) walks through the newsroom's design.
