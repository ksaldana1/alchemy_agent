# alchemy-agent: step plan

## Context

`alchemy_agent` is an empty repo. The goal is an **Effect-native agent framework library** on top of
Alchemy v2 (`alchemy` + `@alchemy.run/cloudflare`). It hides Cloudflare infrastructure behind
agent-level concepts. The philosophy comes from `~/projects/mini-agent-runtime` (ARCHITECTURE.md
principles, FUTURE.md invariants):
- an agent's identity is separate from where it executes
- the platform, not the agent, owns the durable inbox
- delivery is at least once, with idempotency keys
- file access goes through permission checks
- listeners keep working when agents are offline
- waiting must not hold execution capacity
- workflows outlive any single execution

That repo is the **behavioral reference**. Its tests describe behavior to port; its code is not
ported. Per the user's choices, this plan is **written fresh** (CLOUDFLARE_PLAN.md is inspiration
only). **LLM agents are first-class from early on**, the workflow engine is **chosen in a spike**,
and tooling is **Bun**.

Every milestone ends with a **deployed check**: something you can run against a real Cloudflare
stage (`alchemy deploy --stage dev_$USER`) and see working. Most tests run locally
(`Test.make({ dev: true })` on workerd). Each milestone also adds one integration test that deploys
to the cloud.

**End goal:** [NORTH-STAR.md](NORTH-STAR.md) shows the newsroom as it should read once M8 is
done. Each milestone's API should move toward it. Diverge only on purpose, and update that doc
when you do.

## Guiding decisions (proposed; revisit after M0)

| # | Decision | Choice |
|---|---|---|
| D1 | API style | Effect-native throughout. Schemas use Effect `Schema`. Errors are tagged. |
| D2 | Agent = ? | `Agent.make(...)` returns a `Cloudflare.DurableObject` class, one per agent definition. An instance is one agent identity, addressed with `getByName`. |
| D3 | Inbox | A SQLite table inside the agent's DO, owned by framework code. Handlers and LLM tools get services, never raw storage. |
| D4 | Scheduling | `Alchemy.makeCallback` drives the drain loop, delivers outbound messages, and handles retries. Callbacks are scheduled inside the same `storage.transaction` as the state change. Nothing polls. |
| D5 | LLM | Effect AI `LanguageModel` and `Toolkit`. The provider is a Layer: Workers AI by default (no keys), AI Gateway for Anthropic/OpenAI. Tests use a **scripted mock LanguageModel** layer, so they are deterministic. |
| D6 | Agent loop durability | An LLM turn is a **checkpointed state machine**. Each model call and each tool call is a persisted step in the session transcript. The next step is scheduled by a durable callback. A redeploy or eviction resumes from the last checkpoint and never re-runs the whole turn. |
| D7 | Waiting | Waiting on another agent, a person, or a timer **suspends** the session (status `waiting`) and releases the DO. The reply arrives as a new inbox item that resumes it. The transcript is the continuation, so no JS stack has to be resumed. |
| D8 | Files | R2 holds file contents. Grants and the audit live in one `AgentWorkspace` Durable Object (changed from D1 on 2026-11-26; see M4), which checks the grant and then touches R2. |
| D9 | Workflows | **Decided in 0.4 (2026-11-26):** `effect/workflow` (`Workflow.make`, `Activity`, `DurableDeferred`, `DurableClock`) on Alchemy's Durable Object engine (`Cloudflare.Workflows.layerDurableObjectEngine()`). Agent client calls inside a run are durable steps. The native engine runs the same code but has no durable timeouts. |

## Repo layout (Bun workspace)

```
packages/agent/          # the library: "alchemy-agent"
  src/Agent.ts           # Agent.make (Rpc contracts), of / toLayer / client, Agent.generate
  src/AgentModel.ts      # models as Layers (Workers AI, AI Gateway, scripted)
  src/internal/client.ts # clients and the Durable Object binding transport
  src/internal/runtime.ts# the agent's Durable Object: inbox RPCs, drain, step callbacks
  src/internal/inbox.ts  # SQLite inbox, drain loop, outbox
  src/internal/session.ts# LLM turn state machine + transcript
  src/Workspace.ts       # R2 + D1 grants service
  src/AgentWorkflow.ts   # workflow wrapper
  src/Listen.ts          # R2/cron listeners
  src/Platform.ts        # AgentPlatform.make(...) — stack helper: D1, R2, API Worker, queues
  src/Api.ts             # HTTP API Worker + typed client
  test/                  # local (dev:true) suites + type tests
examples/hello/          # M0–M2 playground stack
examples/newsroom/       # acceptance demo (port of mini-agent-runtime/examples/newsroom)
examples/support-desk/   # LLM demo (port of mini-agent-runtime/examples/support-desk)
```

---

## M0: Foundation and spikes (stop and review after this milestone)

> **Status (2026-11-26):** 0.1–0.5 are done (findings in `docs/spike/`); 0.4 decided D9 on 2026-11-26.
> **Earlier (2026-11-25):** 0.1–0.3 and 0.5 were done. D5 and D6 are
> confirmed by 0.5, with caveats about Alchemy's Workers AI adapter (see 0.5's verdict). By decision on
> 2026-11-25, M1 started without 0.4 and 0.5: 0.5 runs before M2 and 0.4 before M5, since only
> those milestones depend on them. D1–D4 are confirmed by 0.2/0.3; D9 stays proposed until 0.4.

**0.1 Scaffold.** Bun workspace. Pin exact versions of `alchemy`, `@alchemy.run/cloudflare`, `effect`,
and `@alchemy.run/test`. Set up `alchemy profile edit --add Cloudflare`. The `examples/hello`
stack has one Worker and a `Counter` DO.
*Deployed check:* `curl $URL/counter/a/increment` works locally (`alchemy dev`) and on
`dev_$USER`, and the count survives a redeploy.

**0.2 Typing spike.** `Agent.make("Echo", { tasks: { echo: Agent.task({ input }, handler) } })`
wraps `Cloudflare.DurableObject<Self>()(...)`. A Worker holds a typed stub and calls `enqueue`.
*Done:* `expectTypeOf` tests prove that task names, inputs, and outputs are inferred, and that a
wrong input is a compile error. If this needs Alchemy internals, record the fallback (an explicit
type param or codegen).

**0.3 Durable callback spike.** Schedule a callback inside a transaction. Throw in it and watch
the retry. Redeploy while a callback is pending.
*Deployed check:* the callback fires after the redeploy, and the retry/backoff behavior is
recorded in `docs/verified-facts.md`.

**0.4 Workflow engine bake-off.** Write the same 2-step workflow in both engines. The workflow
waits for an external signal that a DO sends (`instance.sendEvent` vs `DurableDeferred` signal).
Compare type inference, signaling a running workflow from a DO, replay safety, local dev support,
and limits. Write the decision into D9.

**0.5 LLM spike.** In a DO, `LanguageModel.generateText` uses a `Toolkit` with one tool, through
`Cloudflare.Workers.AI()` → `ai.model(...)`. Also through `Cloudflare.AI.Gateway`. Also build the
scripted mock-model layer.
*Deployed check:* an HTTP call returns a model answer that used the tool. The same test passes
locally with the mock model.

**0.6 Verified facts.** Record limits with source links: DO CPU/wall time per request and
callback, Workers AI tool-calling support per model, R2 → Queue event shape and dedup ID, and the
status of Worker Loader. Also check whether the Cloudflare Agents SDK could be reused for
anything (likely not, since it conflicts with Alchemy DO classes).

---

## M1: Agents with a durable inbox (no LLM yet)

- `Agent.make` / `Agent.task` with Effect Schema input. Helpers: `TaskNames`, `TaskInput`, `TaskOutput`.
- Inbox table in the DO: `task_id` PK, `task`, `input`, `status`, `attempts`, `result`, `error`, timestamps. Migrations use `Cloudflare.SqlMigrations`.
- `enqueue(taskId, task, input)` deduplicates on `taskId`. Invalid input or an unknown task is stored as `failed`, not thrown.
- Drain loop on a durable callback. Statuses: pending → running → completed/failed. Retryable errors back off. `PermanentError` or the attempt cap → failed. A `running` row found after eviction counts as a redelivery (`attempt+1`).
- `AgentContext` services: `agent {type,name}`, `taskId`, `attempt`, and a logger.
- API Worker: `POST /agents/:type/:name/tasks` and `GET .../tasks/:id`, protected by a bearer secret. A typed client: `client.agent(Echo, "a").send("echo", input)` returns a `TaskHandle` with `.result()`.
- Port the scenarios from `mini-agent-runtime/tests/inbox.test.ts` and `agent.test.ts`.

**Deployed check:** a script sends tasks to the deployed stage:
- the happy path works
- a task that throws once succeeds on attempt 2
- a duplicate `taskId` is a no-op
- redeploying while a task is pending still completes it

**Status (2026-11-25): done.** Library in `packages/agent/` (`Agent.ts`, `Api.ts`,
`internal/inbox.ts`); playground stack in `examples/hello/` (stack `Hello`, deployed on
`dev_$USER`). Tests: `packages/agent/test/inbox.test.ts` (15 scenarios, local and on
`test_$USER`) and `test/types/agent.test-d.ts`. The deployed check is
`bun run hello:check <url> all|redeploy-start|redeploy-finish`, and all four checks pass.

Changes from the plan above, and why:
- **No `Cloudflare.SqlMigrations`.** It only reads a directory "relative to where Alchemy runs",
  so a library can't ship its own. The inbox applies an inline, versioned schema on activation
  (`alchemy_agent_schema`), the same approach Alchemy uses for its own alarm tables.
- **`send(task, input, { taskId? })`.** The task id is optional and defaults to a random UUID.
  Agent-to-agent sends inside a handler should pass a derived id (`${ctx.taskId}:…`) so a rerun
  produces the same id; M3 makes that the default.
- **The "logger" in `AgentContext`** is Effect's own logger, annotated with agent, task id and
  attempt. There's no separate service.
- **`handle.result()` polls** the stored record with backoff (100 ms → 2 s) and never gives up
  on its own; callers add `Effect.timeout`. Waiting without polling belongs to M3.
- **Defaults chosen for the 0.2 open questions:** `RuntimeContext` never appears in user-visible
  types. Outputs without an `output` schema must be plain JSON (a compile error otherwise).
  `result()` hangs off the typed `TaskHandle`.
- **API pass (after M1, 2026-11-25):** the API moved to the north-star shape. Calls are
  method-style (`Greeter.named("a").greet(input)` waits; `send.greet` doesn't), and inputs are
  struct fields. `Agent.connect` / `Api.connect` provide how to reach agents. `Api.routes` is
  mountable. Waiting happens inside the Durable Object instead of by polling. The scenarios in
  `inbox.test.ts` carried over (17 now); the deployed checks pass again.
- **Every task carries a `causation`** (`<Type>/<name>/<taskId>` of the sending task, or null
  from the API). This is a start on the observability invariant.

## M2: LLM agents (first-class)

> From 0.5: one step per durable callback (a model call with tool resolution disabled, or one
> tool call), with tool arguments validated in the tool step (`toolCallValidation: "deferred"`).
> Structured output goes through a forced `respond` tool. An empty model response is a retryable
> failure. The live endpoint streams persisted steps, not raw tokens. Tools declare services with
> `Tool.make(..., { dependencies })`. Default models: Llama 3.3 70B or Mistral Small 3.1.

- `Agent.make("Assistant", { model, instructions, tools, tasks? })`. `tools` is an Effect AI `Toolkit` whose handlers run with the agent's services. A built-in task `prompt({ sessionId, message })` runs one **turn**.
- **Sessions:** `sessions` and `transcript` tables in the agent DO. A turn is a loop of steps: `model_call` → zero or more `tool_call` → `model_call`, until a final answer. Each step is persisted before the next callback is scheduled (D6). Tool calls use idempotency key `(sessionId, stepIndex, toolCallId)`. Configurable limits: step cap, token budget, timeout per step.
- Model provider via Layer: `AgentModel.workersAI(modelId)` and `AgentModel.gateway(...)`. Tests provide `AgentModel.scripted([...])`.
- Streaming: `GET /agents/:type/:name/sessions/:id/events` (SSE). It reads the transcript and then tails new steps. Observers reconnect and catch up from the persisted steps.
- Port `support-desk` as the demo (triage → responder), using Workers AI.

**Deployed check:**
- chat with a deployed agent over curl/SSE and see a tool call
- with the scripted model, throwing mid-turn resumes at the failed step, and earlier tool calls do not repeat
- redeploying mid-turn finishes the turn

**Status (2026-11-26): done.** Library: `AgentModel.ts`, `internal/session.ts` (turns and the
step engine), `Agent.prompt` / `Agent.tools` / the built-in `prompt` task in `Agent.ts`, and
session routes in `Api.ts`. Tests: `packages/agent/test/llm.test.ts` (11 scenarios on a scripted
model, local and on `test_$USER`). Demo: `examples/support-desk` (Workers AI, Llama 3.3), deployed
on `dev_$USER`; `bun run support:demo <url> chat|events|tickets|redeploy-start|redeploy-finish`.
All three deployed checks pass (the crash-mid-turn check runs in the cloud suite).

Changes from the plan above, and why:
- **`Agent.prompt` works inside any task, not only the built-in `prompt` task.** The handler
  suspends on an unfinished turn: the task becomes `waiting` and holds no execution. When the
  turn settles, the task is delivered again and the handler reruns from the top, with each
  finished prompt returning its stored answer (the turn id is `<taskId>#<n>`). Code before a
  prompt can therefore run more than once. The north star's Reviewer pattern needs this.
- **Turns queue per session:** one runs at a time, so a transcript never interleaves.
- **Structured output goes through a `respond` tool** built from the stored JSON Schema
  (`Tool.dynamic`). The step engine validates it with `SchemaRepresentation.fromJsonSchemaDocument`,
  and an invalid answer becomes a failed tool result the model corrects. Llama 3.3 needed this on
  the first real ticket.
- **Tool idempotency key:** the `Agent.ToolCall` service gives `idempotencyKey` (session/turn/call).
- **The SSE endpoint ends at `idle`;** clients reconnect with `?after=<last seq>`.
- **Limits:** `maxSteps` (default 24), `maxTokens`, and `stepTimeout` (60 s). An empty model
  response counts as a model error (Alchemy's adapter swallows provider errors): 3 in a row fail
  the turn.
- **Not done:** the support desk's QA judge and the shared files (they need the workspace and
  workflows, M4/M5); `AgentModel.gateway` for Anthropic/OpenAI (waiting on a key, 0.5/L6).

## API reset (after M2, 2026-11-26)

The M1/M2 API had grown its own vocabulary (`Agent.task`, `named(n).task(...)`, `send.*`, `uses`,
`Agent.prompt`, errors flattened to `{ _tag, message }`). It was replaced by Effect's own idioms,
following `effect/cluster`'s `Entity` (see [NORTH-STAR.md](NORTH-STAR.md) and
[ARCHITECTURE.md](ARCHITECTURE.md), "Agents as Rpc groups"):

- **Contracts are `Rpc`s:** `Agent.make("Greeter", [Greet])`, with `payload`, `success`, `error`,
  and `primaryKey`. Contracts live apart from implementations, so callers import only schemas.
- **Implementations:** `X.toLayer(X.of({ … }), { retry, toolkit, limits })`. The Layer declares
  the Durable Object in whichever Worker provides it. The model and the tool handlers are
  requirements that `Layer.provide` supplies.
- **Callers:** `(yield* X.client)(name).Task(payload)`, with `{ discard: true }`. Inside
  Cloudflare the client resolves the binding from the Worker's environment, so `uses` is gone.
  `Api.connect` replaces the transport outside Cloudflare.
- **Errors:** declared errors reach callers as themselves. Platform failures are one `AgentError`
  with a tagged `reason` (`Effect.catchReason`).
- **Generation:** one `Agent.generate({ prompt, system?, session?, schema? })`, typed by its
  schema. A string-encoded schema means a text answer, anything else means the `respond` tool.
- **Per-task retry:** the `Agent.Retry` Rpc annotation.
- **Internals:** `Schema.TaggedError` throughout; stored records are schemas (`TaskRecord`);
  `Effect.fn` / `fnUntraced` instead of functions returning `Effect.gen`; the HTTP client uses
  `HttpClient.retryTransient` and `schemaBodyJson`; SSE frames come from `Sse.encoder`.

Verified: `inbox.test.ts` (16) and `llm.test.ts` (12) locally and on `test_$USER`, the type
tests, Hello's deployed checks (including a pending task across a redeploy), and the support
desk's chat, tickets, and redeploy mid-turn. The existing Durable Object classes upgraded in
place.

Deferred: serving the HTTP API through `HttpApi` (it would give a typed client and OpenAPI for
free, but the routes are generic over agents, so it needs a design pass).

## M3: Agent-to-agent messaging and durable waits

- **Outbox:** `reviewers("junior").Review(payload, { discard: true })` inside a handler writes an outbox row in the sender's transaction. A callback delivers it to the recipient's `enqueue`. The `taskId` is derived from (sender step, send index), so a redelivery never duplicates.
- **Delegate and wait (D7):** waiting on a client call inside a task (today it fails with `WaitingInHandler`), or an auto-generated `ask_<agent>` tool for LLM agents. It sends, then marks the session or task `waiting` and returns. When the recipient completes, it sends a `reply` back to the sender's inbox, which resumes the waiting step with the result. A wait timeout produces a timeout result.
- Sender identity comes from the calling DO, never from a field in the payload. A simple per-agent allowlist controls who can send to whom.
- Lifecycle events (`task.*`, `message.sent`, `session.step`) carry a causation ID and go to structured logs (Workers Logs). Analytics Engine can come later.

**Deployed check:**
- an LLM agent A delegates to B via a tool and finishes with B's answer
- an A→B→A exchange does not deadlock
- killing A mid-wait (redeploy) still resumes once

**Status (2026-11-26): done, except wait timeouts and the allowlist.** Library:
`internal/outbox.ts` (the outbox, its delivery callback, `subscribe` / `reply`), the `Calls`
service in `internal/core.ts`, and its wiring in `internal/runtime.ts` and
`internal/session.ts`. Tests: 6 new scenarios in `inbox.test.ts` and 1 in `llm.test.ts` (34 in
all, local and on `test_$USER`). Deployed checks on `dev_$USER`:
- Hello: `Introduce` waits for another greeter. `WaitForSlow` waits through a redeploy and the
  waiter ends with one attempt (`hello:check <url> redeploy-start|redeploy-finish`).
- Support desk: Helpdesk's `triage_ticket` tool waits for the Triage agent (itself an LLM agent).
- A → B → A runs in the cloud suite.

Changes from the plan above, and why:
- **No `ask_<agent>` tool is generated.** A tool is ordinary code, and its handler can call any
  client and wait, so a hand-written tool covers it with a typed result (see the support desk's
  `triage_ticket`).
- **The send's id comes from the call's position, not a step index.** The n-th call of an
  execution is `<owner>#c<n>`. That makes it the same on a rerun, like `Agent.generate`'s turns.
  A rerun that makes a different call at that position dies with an error that says so.
- **A failed attempt's sends are dropped.** Calls commit with a completed or waiting outcome only.
  The retry makes them again with the same ids.
- **Replies are pushed, not polled.** The recipient keeps subscribers and owes a `reply` through
  its own outbox. `subscribe` is a new RPC, so a recipient still on old code makes the delivery
  retry until the deploy finishes.
- **Sender identity:** causation is set by framework code in the calling Durable Object, and the
  HTTP API always passes none. The per-agent allowlist is deferred until grants exist (M4).
- **Wait timeouts are deferred.** They need a durable timer per waiting call. M7's human requests
  need the same thing, so the two should be built together.
- **Lifecycle logs:** `task.settled` and `message.sent` with causation, through Effect's logger
  (Workers Logs). `session.step` isn't logged yet.

## M4: Workspace (permissioned files)

- Logical paths `~/x`, `agents/<type>/<name>/x`, and `shared/...` map to R2 keys. `..` and absolute keys are rejected.
- A D1 `grants` table (`path_prefix`, `grantee`, `access`). Owners implicitly have `~/`. `share` and `revoke`. Only the platform grants access to `shared/`. The most specific grant wins. Every allow and deny goes to an audit table.
- A `Workspace` service (`read`, `write`, `list`, `share`, `revoke`) for task handlers, plus a ready-made `Workspace.toolkit` that LLM agents can include. `FsDeniedError` is permanent.
- Port the scenarios from `mini-agent-runtime/tests/filesystem.test.ts`.

**Deployed check:** an LLM agent reads a shared doc, writes `~/summary.md`, and shares it with a
second agent, which can read it. A third agent is denied, and the denial is audited.

**Design (decided 2026-11-26, before starting M4):**
- **Grants live in one `AgentWorkspace` Durable Object, not D1 (D8 changes).** Its SQLite holds
  `grants` and `audit`, so a check and its audit row commit together, and a revoke can't race a
  read. It also does the R2 operation after the check, so the bucket is bound to it alone. Why not
  D1: Alchemy's D1 client turns SQL errors into defects (we never catch those), its migrations only
  read a directory (the `SqlMigrations` problem again), and a single D1 database is just as much a
  single writer. One instance named `workspace` for now; sharding by owner can come later if it
  becomes a bottleneck.
- **The library declares the infrastructure.** `Workspace.layer({ grants })` declares the
  `AgentWorkspace` Durable Object and the R2 bucket (`AgentWorkspaceFiles`) in the Worker that
  provides it, next to the agents. Agent code never names either one.
- **Identity:** the `Workspace` service reads the caller's address from `AgentContext` (framework
  code in the agent's Durable Object) and passes it on the RPC. Agent code never supplies it.
- **Platform grants are declared:** `Workspace.layer({ grants: [Grant.read(Editor.address("desk"), "shared/desk/")] })`.
  They are seeded when the Durable Object activates: added, updated, and removed if no longer
  declared. Shares made by agents aren't affected. `Platform.make({ grants })` absorbs this later.
  The HTTP API gets platform file routes (`PUT`/`GET /workspace/files/shared/...` and
  `GET /workspace/audit`), so scripts can upload a shared doc and inspect decisions.
- **`FsDenied` is always permanent.** If the Rpc declares it, it reaches the caller as itself
  (the north star's `catchTag("FsDenied")`). Otherwise the task fails with
  `AgentError(reason: FsDenied)`. It's added to `AgentErrorReason` and never retried.
- **Writes return `Workspace.FileRef`** (`{ path, revision, size }`, where `revision` is the R2
  etag). `Workspace.revision(path)` reads it, which prepares M7's revision-bound approvals.
- **Audit rows can repeat on a rerun.** Code before a waiting call can run more than once. The
  audit is a log, not state, and each row carries the task id and attempt, so repeats can be told
  apart. Writes overwrite, and `share` is an upsert, so repeating them changes nothing.
- **Still deferred:** the per-agent allowlist of who may call whom. It's about messages, not
  files, so it moves to `Platform.make`.

**Status (2026-11-26): done.** Library: `Workspace.ts` (operations, `FileRef`, `FsDenied`,
`FileNotFound`, `Toolkit` / `ToolkitLayer`, `layer`), `Grant.ts`, `internal/paths.ts` (paths and
decisions, pure), `internal/workspace.ts` (the Durable Object), workspace routes in `Api.ts`
(`/workspace/files|list|audit`), and `FsDenied` as an `AgentError` reason. Tests:
`test/paths.test.ts` (12, pure) and `test/workspace.test.ts` (10, local and on `test_$USER`),
which port both halves of `mini-agent-runtime/tests/filesystem.test.ts` (not the symlink case:
R2 has no links). Deployed check on `dev_$USER`: `bun run support:demo <url> workspace`. The
support desk's Scribe (Llama 3.3, workspace tools) reads `shared/kb/…`, writes `~/summary.md`,
and its handler shares it with `Reader/billing`, which reads it; `Reader/intruder` gets
`FsDenied`, and the denial is in the audit.

Changes from the design above, and why:
- **The Scribe's handler shares the summary, not the model.** `share_file` is in the toolkit
  (and tested with the scripted model), but sharing is a decision the handler can make
  deterministically, as the north star's Editor does with its `Share` task.
- **Tool denials are returned, not raised** (`failureMode: "return"`): the model sees
  `_tag: "FsDenied"` and the reason, instead of only a message.
- **`FileNotFound` is retryable** (a file may appear later), unlike `FsDenied`.

## M5: Workflows

> From 0.4: workflows are plain `effect/workflow` code on the Durable Object engine, and an agent
> client call inside a run is already a durable step (`internal/workflow.ts`). So most of the
> `AgentWorkflow` wrapper below isn't needed. M5 is left with: wiring the engine (and its
> provision to agents) into hosting, `waitFor` with a timeout over `DurableDeferred.raceAll`,
> the API routes for runs and signals, skipping the workflow path inside an `Activity`, and the
> newsroom's `publish`.

- `AgentWorkflow.make(name, { input, run })` on the engine chosen in M0.4.
- `AgentWorkflow.call(Agent, task, input)` enqueues with a deterministic `taskId` (instance ID + call index). The agent signals completion, the step waits and returns the typed result, or fails with `TaskFailedError`. Parallel calls go through `Effect.all`.
- `sleep`, plus `AgentWorkflow.waitFor(name, schema, timeout)` for approvals sent through the API.
- The API can start a workflow, get its status, and send it a signal.

**Deployed check:**
- the newsroom `publish` workflow (edit → two parallel reviews) completes on the stage
- a redeploy mid-wait is survived
- an approval arriving after a timeout is handled correctly

**Status (2026-11-26): done.** Library: `Platform.ts` (`Platform.layer` / `Platform.make`: agents,
workflows on the Durable Object engine, the workspace, and the API Worker, with the engine given
to the agents), `Signal.ts` (`Signal.make`, `Signal.await(signal, { timeout })` failing with
`SignalTimeout`), workflow routes in `Api.ts` (`/workflows/:name/runs[/:id[/signals/:signal]]`),
and `internal/workflow.ts` (agent calls in a run, the call-position guard, no agent calls inside
an `Activity`, the engine registry). Tests: `test/workflow.test.ts` (8, local; 7 on
`test_$USER`, since the position guard only runs locally). The test Worker is built with
`Platform.layer`. Demo: `examples/newsroom` (Workers AI), deployed on `dev_$USER`;
`bun run newsroom:demo <url> publish|approve|status`. All three deployed checks pass:
- publish: intake → LLM edit → share → two reviews at once (the junior one is denied with
  `FsDenied` and caught; the senior one approves) → sign-off → publish. Each step runs once.
- a `--force` redeploy while the run waited for sign-off: the approval resumed it, and the
  article was published
- a sign-off window of 20 s with no answer: the run fails with `Rejected(by: "timeout")`, and an
  approval sent afterwards changes nothing

Changes from the plan above, and why:
- **No `AgentWorkflow` module.** Workflows are plain `effect/workflow` (`Workflow.make`,
  `Workflows.layer`, `Activity`, `DurableClock`), and an agent client inside a run is a durable
  step (0.4). Agent failures are the client's usual errors: declared ones as themselves, the
  rest as `AgentError`.
- **`Signal` instead of `waitFor`:** a signal is a typed `DurableDeferred`, sent over
  `POST …/signals/:name`. The timeout is a durable race. M7's human requests build on it.
- **`Platform.make` came early** (planned for later), because three things needed central
  wiring: the engine (agents must have it in their context to reply to runs), the workspace and
  its grants, and the API. It takes `main` (the module that default-exports the Worker), so the
  platform lives in its own file and the stack yields it.
- **The engine stores a defect as `{ name, message }`.** An `AgentError` has no message, so a
  workflow that lets one become a defect should say why. The newsroom's does.
- **Unexplained:** one cloud run of "starting the same key twice" ended `Died`, about 12 s after
  a fresh deploy. Two later cloud runs and 12 simultaneous duplicate starts on a warm stack all
  completed. The test now prints the whole status, so the defect will show if it happens again.

## M6: Listeners

- `Listen.r2(bucket, { prefix })` provisions an R2 → `Queues.Subscription` → Queue → consumer, plus a dead-letter queue. The consumer enqueues to an agent task, or starts a workflow, using the R2 event identity as the idempotency key. Declared in `AgentPlatform.make({ listeners })`.
- `Listen.cron(expr)` uses the scheduled time in the idempotency key.

**Deployed check:** uploading a file to `incoming/` starts exactly one workflow, even when the
same event is replayed by hand into the queue. A poison message lands in the DLQ and emits
`listener.error`.

## M7: Humans in the loop and inspection

- Human requests: a typed response schema, states pending/resolved/expired/canceled, and a binding to an artifact revision (R2 etag). `POST /requests/:id/respond`. Resolving resumes the waiting session or workflow through the M3 mechanism.
- A D1 registry/projection built from callbacks (eventually consistent): `GET /agents`, the inbox per agent, task history, session transcripts, and workflow status.
- Optional: hibernatable WebSocket live feed.

**Deployed check:**
- a person approves a draft through HTTP after a redeploy, and work resumes once
- a duplicate response is a no-op
- an approval of a stale revision is rejected

## M8: Acceptance demo
The target code for this demo is in [NORTH-STAR.md](NORTH-STAR.md).

The newsroom on Cloudflare, with LLM editor and reviewers:
1. A file uploaded to R2 under `incoming/` triggers a listener.
2. The listener starts the publish workflow.
3. The editor (LLM + Workspace tools) edits the file.
4. Two reviewers review it.
5. A human approves.
6. The result is published.

Scripted chaos: a handler throws mid-turn, a redeploy happens mid-run, and the same R2 event is
delivered twice. Each logical step must take effect once. The agent and workflow code must
contain no infrastructure code and be shorter than `mini-agent-runtime/examples/newsroom/`.

## Later

- Generated agents through Worker Loader (`globalOutbound: null`) with a scoped ctx
- Versioned definitions with sessions pinned to versions
- An async/await facade
- Publishing the package to npm

---

## Verification approach

- **Type tests** (`expectTypeOf`) for every public API: `bun test packages/agent/test/types`.
- **Local suites** use `@alchemy.run/test/Bun` with `Test.make({ dev: true })`. They run on workerd with the local R2, D1, Queues, and Workflows simulators and the scripted model. This is the fast loop for each milestone.
- **Cloud integration** runs one suite per milestone with `deploy(Stack)` to `test_$USER`, plus `destroy` in CI. These are the "deployed checks" above. They use Workers AI for real model calls.
- **Manual smoke:** `examples/*` deploy with `alchemy deploy --stage dev_$USER`, and `scripts/smoke-mN.ts` drives each milestone over HTTP.
- Review gates: stop after M0 to revisit D1–D9, and after each milestone for review.
