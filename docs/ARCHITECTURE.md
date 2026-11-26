# Architecture

> **Status (2026-11-26):** principles, invariants, and what M1–M6 and the API reset built. The
> rest of the component structure gets added as milestones land. The "Guiding decisions" table in
> [PLAN.md](PLAN.md) is the working design for what isn't built yet.
> Terms are defined in [GLOSSARY.md](GLOSSARY.md).
> The end goal, as code, is in [NORTH-STAR.md](NORTH-STAR.md).

## Purpose

alchemy-agent is a framework for persistent, cooperating agents on Cloudflare, built with
[Alchemy](https://alchemy.run) and Effect. An agent is a long-lived identity with an inbox, a
home in a shared workspace, and (optionally) a model and tools. The platform delivers work,
enforces permissions, reacts to events, and runs workflows. Agent code describes *what* an agent
does and never deals with infrastructure.

The ideas come from `mini-agent-runtime`, a local reference implementation: agents behave like
Erlang actors or OS processes, and the platform acts like a small operating system around them.
That repo's tests describe behavior worth keeping. Its code is not ported.

## Principles

1. **Identity is separate from execution.** An agent exists, keeps its inbox, workspace, sessions,
   and history, and can be addressed whether or not any of its code is running. Executions are
   temporary and invisible to agent code.
2. **The platform owns obligations.** Inboxes, transcripts, workflow progress, and human requests
   belong to the platform. Agent code receives work and reports outcomes. It never manages the
   records that say what is owed.
3. **Agents communicate by messages.** Agents send and call through the platform. They don't share
   memory or reach into each other's state. Files in the workspace are the only other shared
   medium, and they are permissioned.
4. **Everything that matters is durable, and resumes rather than restarts.** A task, a turn, or a
   workflow run interrupted at any point continues from its last recorded step. Nothing depends on
   a process or connection staying alive.
5. **Waiting costs nothing.** Work that is waiting on a reply, a person, a signal, or a timer holds
   no execution. It is resumed by what it was waiting for.
6. **Access is checked by the platform, from identity it determines itself.** Who is asking comes
   from where the request originates, never from a field the caller fills in. Every workspace
   access is checked against grants, and every decision is audited.
7. **Events and workflows don't depend on agents being active.** Listeners create work for idle
   agents. Workflows outlive every agent execution involved in them.
8. **LLM agents are first-class but not special to the platform.** A model call and a tool call
   are steps like any other: recorded, resumable, and subject to the same delivery and permission
   rules. The model is a replaceable dependency.
9. **Types flow from definitions.** An agent or workflow definition is the single source of truth
   for task names, inputs, and outputs. Every call site is typed from it, and inputs are checked
   against it at runtime.
10. **Agent code describes behavior, not infrastructure.** Agent and workflow code never names
    buckets, queues, databases, or bindings. The platform provisions and wires those from the
    declarations.

## Invariants

These must hold in every implementation. Each one should have a test.

**Delivery**
- A task is recorded at most once per task id. Creating it again has no effect.
- Delivery is at least once. A task's effects inside the platform take effect once. Effects
  outside the platform may repeat, and agent code uses the task id as the idempotency key for them.
- A settled task never runs again, and its outcome never changes.
- Every task eventually settles: completed, failed permanently, or failed after its attempt cap.
  Nothing retries forever.

**Atomicity**
- A state change and the scheduling of its follow-up work commit together, or neither does.
- A send is recorded together with the outcome of the step that made it. A step that runs again
  produces the same task ids and therefore no duplicates.

**Resumption**
- A turn resumes from its last recorded step. A recorded tool call is never repeated because a
  later step failed.
- Waiting work resumes exactly once per reply, response, or signal, even if that arrives more than
  once.

**Authorization**
- An agent's identity in any request is determined by the platform, never by the caller.
- Only the platform grants access to the shared area. Agents can share only from their own home,
  and shared grants cannot be passed on.
- An approval applies only to the revision it was given for.

**Observability**
- A transition is recorded before anyone is told about it. Observers who reconnect catch up from
  the record and miss nothing.
- Every task carries its causation, so any chain of work can be reconstructed after the fact.

## Trust boundary

Code is split into **framework code** and **agent code** (task handlers, tool implementations,
workflow bodies). Framework code owns storage, delivery, scheduling, and permission checks. Agent
code reaches the outside world only through services the framework hands it.

This boundary is enforced by the API, not by isolation. Agent code is trusted not to bypass it.
Running untrusted or generated agent code inside an isolated sandbox with a scoped set of
capabilities is planned later (see PLAN.md, "Later").

## Out of scope

- Multi-tenancy, user accounts, and authentication beyond a shared secret for the API.
- Chat UI and client state sync.
- Running on providers other than Cloudflare.

## Agents as Rpc groups (the API reset, 2026-11-26)

**Contract.** An agent's contract is a list of `effect/rpc` `Rpc`s: `Agent.make("Greeter", [Greet,
Ping])`. Each Rpc's `payload`, `success`, and `error` schemas type every call site (principle 9),
and the wire format is `Schema.toCodecJson` of them. `primaryKey` gives a deterministic task id
(`<tag>:<key>`), so sending the same key twice records one task. A per-task retry policy is an
annotation: `.annotate(Agent.Retry, { delay: "30 seconds" })`.

**Hosting.** `X.toLayer(X.of({ … }), { retry, toolkit, limits })` returns a Layer that declares the
agent's Durable Object (Alchemy's declaration form, `DurableObject<Self, Shape>()(type).make(…)`)
in the Worker that provides it. The Layer's requirements are the handlers' requirements, minus the
services the framework provides (`AgentContext`, generation), plus `LanguageModel` and the
toolkit's handlers if the agent has tools. `Layer.provide` supplies those. The toolkit is an
option on the agent, not on each call: a turn outlives the execution that started it, so the
tools have to be known whenever the Durable Object wakes.

**Errors.** A handler's declared errors (the Rpc's `error` schema) settle the task permanently and
reach the caller as themselves, so `catchTag` works across agents. Everything else the platform
reports is one `AgentError { agent, reason }`, where the reason is a tagged union: `Unreachable`,
`ExecutionLost`, `RetriesExhausted`, `InvalidPayload`, `UnknownTask`, `GenerationFailed`,
`FsDenied` (an undeclared workspace denial, never retried). (Records
from before M3 can also carry `WaitingInHandler`, which nothing produces any more.) An undeclared
failure is retried up to the attempt cap.

## The inbox (implemented in M1)

**Storage.** Each agent is one Durable Object instance, and its inbox is the SQLite table
`alchemy_agent_inbox` inside it. A row holds `task_id` (unique), `task`, the encoded `input`,
`status` (`pending` / `running` / `waiting` / `completed` / `failed`), `attempts`, `max_attempts`, `run_at`, `result`, `error`, `causation`, and timestamps.
`alchemy_agent_meta` holds the drain and step cursors, and `alchemy_agent_schema` holds the schema version.
Alchemy's own job table holds the one pending `drain` callback.

**Creating a task.** `enqueue` runs in one `storage.transaction`. It decodes the payload
(an unknown task or an invalid payload is stored as `failed` right away), inserts the row (an existing
task id is left untouched), and schedules the `drain` callback for now. A write and the scheduling
of its follow-up commit together.

**The drain loop.** A durable callback, single-flight per Durable Object:
1. Any row still `running` belongs to an execution that ended without settling it. It counts as a
   lost attempt (`ExecutionLost`): back to `pending` with a backoff, or `failed` at the cap.
2. Up to 10 due rows, FIFO. For each: mark `running` and increment `attempts` (committed
   before the handler runs), run the handler, then commit one of: `completed` with the result,
   `failed` (a declared error, a failed generation, or the attempt cap), `waiting` (the handler
   reached an unfinished LLM turn), or `pending` with `run_at = now + delay × 2^(attempts−1)`.
   The policy is the defaults, then the agent's `retry`, then the task's `Agent.Retry`
   annotation.
3. If any row is still pending, reschedule the drain at the earliest `run_at`, passing an
   increasing cursor as `progress`. Without it, Alchemy counts each reschedule as a retry.

The drain never fails because of agent code. Handler failures are recorded in the row, so the
inbox, not Alchemy, decides about retries, and every task settles. Alchemy retries the drain
callback itself only when the execution dies (for example, the CPU limit), and step 1 recovers.

**Callers.** `(yield* Greeter.client)("a").Greet(payload)` sends and waits for the result;
`{ discard: true }` only sends. How the client reaches the agent comes from the `Agents`
reference:
- by default, inside Cloudflare, the Durable Object binding, resolved from the Worker's
  environment at call time (so any Worker that hosts the agent can call it, handlers included)
- outside Cloudflare, `Api.connect({ url, token })` replaces it with the HTTP API (mounted with
  `Api.routes`)

Inside an agent (a handler, or a tool during a turn), calls go through the agent's outbox instead
(see "Messages between agents" below). Outside one, waiting happens inside the recipient's
Durable Object: a `wait` RPC parks on an in-memory waiter released when the settling transaction
commits, and returns after 20 s at most. Callers loop, so an eviction only costs a re-read.

**Deploys and version skew.** Right after a deploy, a Durable Object instance can still run the
previous code for a few minutes. Callers must tolerate an instance that is one version behind:
add RPC methods without removing old ones, and have callers fall back when a new method is
missing. (`wait` falls back to reading the record.)

## Messages between agents (implemented in M3)

**Storage.** Two more tables in each agent's Durable Object. `alchemy_agent_outbox` holds the
calls the agent makes (`kind = call`) and the replies it owes to others (`kind = reply`), each
with a delivery status. `alchemy_agent_subscribers` holds, per task, who waits for its reply.

**A call.** Inside an agent, the client hands each call to the `Calls` service. The n-th call of
an execution is outbox row `<owner>#c<n>`. The owner is the task id for a handler, or
`<turnId>/<toolCallId>` for a tool. The recipient's task id comes from the Rpc's `primaryKey`,
or else is `<Type>/<name>/<row id>`. Either way a rerun produces the same ids, so it never sends
twice. A handler must therefore make the same calls in the same order on every run. If a rerun
makes a different call at the same position, it dies with an error that says so.

- **Recorded with the outcome.** New calls are buffered during an execution and written in the
  transaction that records its outcome: the task completing or suspending, or the tool's
  result. A failed execution's calls are dropped; the retry makes them again. A call already
  recorded returns at once: `undefined` for a discard, its recorded reply once it has one.
- **Calls wait.** A call whose reply isn't recorded yet makes its owner wait: the
  task becomes `waiting`, or the turn does when a tool made the call. Waiting holds no
  execution. Concurrent calls (`Effect.all`) are all recorded before the owner starts waiting.

**Delivery.** A durable callback (`alchemy-agent/outbox`) delivers due rows in order. For a call
it runs `enqueue` on the recipient, and for a waiting call then `subscribe`. If the task has
already settled, `subscribe` returns it and the reply is recorded at once. Otherwise the
recipient records the subscriber. A failed delivery backs off, from 1 s up to 5 min. After 10
failures, a waiting call gets `AgentError(Unreachable)` as its reply. A recipient still on
pre-M3 code has no `subscribe`, so delivery fails and retries until the deploy finishes.

**Replies.** The transaction that settles a task also turns its subscribers into `reply` rows in
the recipient's outbox. Delivering one calls `reply` on the waiting agent. That records the
reply on the call's row and resumes the owner, once, in one transaction: the task goes back to
`pending`, or the turn schedules its next step. The handler (or the tool) then runs again and
gets the recorded reply. Replies need the recipient's Worker to have the caller's binding,
which holds when both agents are hosted in the same Worker.

Because waiting holds nothing, agents can wait on each other in both directions (A → B → A)
without deadlocking. The tests cover that, a parallel fan-out, a declared error crossing a wait,
a waiting call to a task that already settled, and a crash after the reply arrived. Hello's
deployed check covers a redeploy while an agent waits: the waiter has exactly one attempt
afterwards.

Lifecycle logs: `task.settled` and `message.sent` carry the causation. They go to Workers Logs.

Not done yet: wait timeouts, and a per-agent allowlist of who may call whom.

## The workspace (implemented in M4)

**Hosting.** `Workspace.layer({ grants })` declares one more Durable Object class,
`AgentWorkspace`, and an R2 bucket, `AgentWorkspaceFiles`, in the Worker that provides it (next
to the agents). Only the workspace object binds the bucket. One instance, `workspace`, serves
everyone.

**Storage.** The object's SQLite holds `alchemy_workspace_grants` (`grantee`, `path`, `access`,
`granted_by`) and `alchemy_workspace_audit` (every decision: caller, op, path, allowed, reason, and
the task id and attempt that asked). R2 holds file contents under their logical path.

**Paths** (`internal/paths.ts`, pure). `~/x` expands to `agents/<type>/<name>/x`. `.` and `..` are
resolved before any check. Absolute paths, escapes, the root, and anything outside `agents/` and
`shared/` are rejected (and audited as denials).

**Decisions.** The platform (the HTTP API) may do anything. An owner may do anything in its home.
Otherwise the most specific grant covering the path decides; a grant on `p` covers `p` and
`p/…`, never a sibling such as `p2`. Only an owner shares or revokes, and only inside its home,
so shares can't be passed on and only declared grants reach `shared/`. Declared grants are
replaced on every activation (`granted_by = 'platform'`); agents' shares are left alone.

**An operation** is one RPC from framework code in the calling agent's Durable Object. The
caller's address comes from `AgentContext`, never from agent code. The object normalizes, decides,
and writes the audit row in one transaction, then touches R2. Results cross the RPC as data, and
the client turns a denial into `FsDenied` and a missing file into `FileNotFound`. A write returns a
`FileRef` whose `revision` is R2's etag.

**Denials are permanent.** A declared `FsDenied` settles the task and reaches the caller as
itself. An undeclared one settles it with `AgentError(reason: FsDenied)`, without retries. In a
turn, the workspace tools (`Workspace.Toolkit`) use `failureMode: "return"`: the model sees the
typed failure and can carry on.

**Reruns.** Code before a waiting call can run more than once, so its workspace operations can
repeat. Writes overwrite and shares upsert, so the state is the same; the audit can show the
repeat, with the same task id.

## Workflows (implemented in M5, engine chosen in spike 0.4)

**Code.** A workflow is plain `effect/workflow`: `Workflow.make(name, { payload, success, error,
idempotencyKey })`, implemented with `Workflows.layer(W, handler)` (Alchemy), using `Activity`,
`DurableClock.sleep`, and `Signal.await` (a `DurableDeferred`, optionally raced against a durable
clock). An agent client call in the workflow body is a durable step.

**Engine (D9).** Alchemy's Durable Object engine: one Durable Object class per workflow
(`<Name>Workflow`), one instance per run, named by its execution id, with the run's journal in its
SQLite. A run replays from the top on each wake-up: journaled steps return their results, the
first new one runs, and a wait that isn't ready suspends the run (holding nothing).

**An agent call in a run** (`internal/workflow.ts`). The n-th call is an Activity
`agent-call/c<n>` that enqueues the task (id `wf/<Workflow>/<executionId>/c<n>`, or the Rpc's
`primaryKey`) and subscribes with reply-to `workflow:<Workflow>` / `<executionId>` /
`agent-call/c<n>`. The Activity records which call it made, so a replay that makes a different
call there dies instead of reusing another call's answer. The run then awaits the
`DurableDeferred` `agent-call/c<n>`. When the task settles, the agent's outbox delivers the reply
with the engine's `deferredDone`. That's why the engine must be in the agents' context, which
`Platform.layer` arranges. An agent call inside an `Activity` dies: calls belong in the workflow
body.

**Hosting** (`Platform.layer` / `Platform.make`). The agents, the workflows, and the workspace
are built with the engine provided (`Layer.provideMerge`). The engine is wrapped to record
which workflows register with it, so the HTTP API can start runs, read them, and signal them by
name.

**Signals.** `POST /workflows/:name/runs/:id/signals/:signal { value }` completes the run's
deferred with the encoded value; the run decodes it with its own schema. The first value wins,
and later ones (or one after the timeout won the race) are ignored. A value sent before the run
reaches the wait is kept.

## Listeners (implemented in M6)

**Declaring.** `Listen.files(prefix, { suffix })` (files written under a logical workspace path)
or `Listen.cron(expr)`, each with `.start(workflow, map)` or `.send(agent, name, task, map)`, in
`Platform.make({ listeners })`. A listener has a name (default `files:<prefix><suffix>` or
`cron:<expr>`).

**One queue per platform.** `AgentListenerEvents` carries:
- the R2 notifications for the workspace bucket, one rule per listener key range (Cloudflare
  rejects overlapping ranges in one configuration, so listeners on overlapping prefixes won't
  deploy)
- cron fires: the Worker's scheduled handler puts `{ kind: "cron", listener, cron,
  scheduledTime }` on the queue
- replays: `POST /listeners/events`

The consumer (batches of 1) decodes each message, matches it to its listeners, maps the event to
a payload, and starts the run (`workflow.execute(..., { discard: true })` on the engine) or
enqueues the task on the agent (causation `listener/<name>`). Any failure (a mapping that throws,
a payload that doesn't decode, an unreachable agent) retries the message: 3 retries, 2 s
apart. Then it goes to `AgentListenerDeadLetters`, whose consumer logs `listener.error` and
records it.

**Exactly once.** The queue is at least once, and listeners don't dedupe. The target does: a
run's execution id comes from the workflow's `idempotencyKey` of the payload, and a task's id
from the Rpc's `primaryKey`, or else `listener/<name>/<event id>`, where the event id is
`<path>@<etag>` or the fire's `scheduledTime`. A redelivered or replayed event recreates nothing.

**The log.** The `AgentListeners` Durable Object records each delivery (with the run or task it
created), each failure, and each dead letter (`GET /listeners/log`). Writing to it is best
effort and never fails a delivery.

## LLM turns (implemented in M2)

**Storage.** Two more tables in the agent's Durable Object. `alchemy_agent_turns` holds one row
per prompt: status `queued` / `running` / `waiting` (a tool waits for another agent) / `completed` / `failed`, the message, the output schema,
the system prompt, step, error and token counters, and the output. `alchemy_agent_transcript` holds one row per
message (`user`, `model`, `tool_result`) per session, encoded with Effect AI's `Prompt.Message`
schema.

**A generation.** `Agent.generate({ prompt, system?, session?, schema? })` inside a handler looks
up turn `<taskId>#<n>`. A finished turn returns its answer, decoded with the schema (default
`Schema.String`). Otherwise, in one transaction, the turn is
recorded and started if its session is free (the user message is appended and the first step
scheduled). The handler then suspends (a defect, so handler error handling can't swallow it),
and the task becomes `waiting`.

**A step** (durable callback `alchemy-agent/step`, job id = turn id) does exactly one of:
- One model call with tool resolution disabled and deferred argument validation, persisted as a
  `model` row.
- One tool call, persisted as a `tool_result` row. Invalid arguments and typed tool failures
  become failed results the model sees.
- Settling the turn: its text, or a valid `respond` call.

How the model answers depends on the schema. If its encoded form is a string, the model answers in
text, which the schema decodes. Anything else goes through a `respond` tool built from the
schema's JSON Schema. An answer that doesn't decode goes back to the model with the error, inside
the same turn. The schema itself is kept in memory to validate. After an eviction, the JSON
Schema document stored with the turn validates instead. The step cap and the token budget end a
turn with `GenerationError` (`StepLimit` / `TokenBudget`).

Each step schedules the next with `progress` from one counter for the whole Durable Object,
because Alchemy compares a new job's progress with the scheduling job's, even across job ids.
Settling resumes the waiting task (`waiting` → `pending`) and starts the session's next queued
turn, in one transaction.

**Watching.** Every persisted row and turn change is pushed to in-memory listeners. `GET
…/sessions/:id/events` replays the transcript after `?after=`, then streams new rows until the
session is idle.

## To be added after M0

- Components and responsibilities, and how they map onto Cloudflare and Alchemy resources.
- Data flow for: creating a task, the drain loop, send/call/reply, an LLM turn, listener → workflow,
  and human request → resume.
- Storage layout: what is stored per agent, what is global, and what is in the workspace.
- Failure handling: what happens on retry, redeploy, eviction, and duplicate events.
