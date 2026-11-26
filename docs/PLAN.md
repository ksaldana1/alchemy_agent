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
| D8 | Files | R2 holds file contents. Grants live in D1. The framework checks the grant inside the caller's DO before touching R2. |
| D9 | Workflows | Pick in M0.4: native `Cloudflare.Workflow` vs `effect/workflow` on `layerDurableObjectEngine`. The public `AgentWorkflow` API is the same either way. |

## Repo layout (Bun workspace)

```
packages/agent/          # the library: "alchemy-agent"
  src/Agent.ts           # Agent.make, Agent.task, Agent.tool, type helpers
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

> **Status (2026-11-25):** 0.1–0.3 are done (findings in `docs/spike/`). By decision on
> 2026-11-25, M1 started without 0.4 and 0.5: 0.5 runs before M2 and 0.4 before M5, since only
> those milestones depend on them. D1–D4 are confirmed by 0.2/0.3; D5, D6 and D9 stay proposed
> until their spikes run.

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

- `Agent.make("Assistant", { model, instructions, tools, tasks? })`. `tools` is an Effect AI `Toolkit` whose handlers run with the agent's services. A built-in task `prompt({ sessionId, message })` runs one **turn**.
- **Sessions:** `sessions` and `transcript` tables in the agent DO. A turn is a loop of steps: `model_call` → zero or more `tool_call` → `model_call`, until a final answer. Each step is persisted before the next callback is scheduled (D6). Tool calls use idempotency key `(sessionId, stepIndex, toolCallId)`. Configurable limits: step cap, token budget, timeout per step.
- Model provider via Layer: `AgentModel.workersAI(modelId)` and `AgentModel.gateway(...)`. Tests provide `AgentModel.scripted([...])`.
- Streaming: `GET /agents/:type/:name/sessions/:id/events` (SSE). It reads the transcript and then tails new steps. Observers reconnect and catch up from the persisted steps.
- Port `support-desk` as the demo (triage → responder), using Workers AI.

**Deployed check:**
- chat with a deployed agent over curl/SSE and see a tool call
- with the scripted model, throwing mid-turn resumes at the failed step, and earlier tool calls do not repeat
- redeploying mid-turn finishes the turn

## M3: Agent-to-agent messaging and durable waits

- **Outbox:** `Agent.send(Reviewer.named("junior"), "review", input)` writes an outbox row in the sender's transaction. A callback delivers it to the recipient's `enqueue`. The `taskId` is derived from (sender step, send index), so a redelivery never duplicates.
- **Delegate and wait (D7):** `Agent.call(...)` inside a task, or an auto-generated `ask_<agent>` tool for LLM agents. It sends, then marks the session or task `waiting` and returns. When the recipient completes, it sends a `reply` back to the sender's inbox, which resumes the waiting step with the result. A wait timeout produces a timeout result.
- Sender identity comes from the calling DO, never from a field in the payload. A simple per-agent allowlist controls who can send to whom.
- Lifecycle events (`task.*`, `message.sent`, `session.step`) carry a causation ID and go to structured logs (Workers Logs). Analytics Engine can come later.

**Deployed check:**
- an LLM agent A delegates to B via a tool and finishes with B's answer
- an A→B→A exchange does not deadlock
- killing A mid-wait (redeploy) still resumes once

## M4: Workspace (permissioned files)

- Logical paths `~/x`, `agents/<type>/<name>/x`, and `shared/...` map to R2 keys. `..` and absolute keys are rejected.
- A D1 `grants` table (`path_prefix`, `grantee`, `access`). Owners implicitly have `~/`. `share` and `revoke`. Only the platform grants access to `shared/`. The most specific grant wins. Every allow and deny goes to an audit table.
- A `Workspace` service (`read`, `write`, `list`, `share`, `revoke`) for task handlers, plus a ready-made `Workspace.toolkit` that LLM agents can include. `FsDeniedError` is permanent.
- Port the scenarios from `mini-agent-runtime/tests/filesystem.test.ts`.

**Deployed check:** an LLM agent reads a shared doc, writes `~/summary.md`, and shares it with a
second agent, which can read it. A third agent is denied, and the denial is audited.

## M5: Workflows

- `AgentWorkflow.make(name, { input, run })` on the engine chosen in M0.4.
- `AgentWorkflow.call(Agent, task, input)` enqueues with a deterministic `taskId` (instance ID + call index). The agent signals completion, the step waits and returns the typed result, or fails with `TaskFailedError`. Parallel calls go through `Effect.all`.
- `sleep`, plus `AgentWorkflow.waitFor(name, schema, timeout)` for approvals sent through the API.
- The API can start a workflow, get its status, and send it a signal.

**Deployed check:**
- the newsroom `publish` workflow (edit → two parallel reviews) completes on the stage
- a redeploy mid-wait is survived
- an approval arriving after a timeout is handled correctly

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
