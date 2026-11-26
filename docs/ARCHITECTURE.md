# Architecture

> **Status (2026-11-25):** principles and invariants only. Component structure, data flow, and
> the mapping onto Cloudflare resources get added after the M0 spikes confirm decisions D1–D9 in
> [PLAN.md](PLAN.md). Until then, the "Guiding decisions" table in PLAN.md is the working design.
> Terms are defined in [GLOSSARY.md](GLOSSARY.md).

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

## The inbox (implemented in M1)

**Storage.** Each agent is one Durable Object instance, and its inbox is the SQLite table
`alchemy_agent_inbox` inside it. A row holds `task_id` (unique), `task`, the encoded `input`,
`status`, `attempts`, `max_attempts`, `run_at`, `result`, `error`, `causation`, and timestamps.
`alchemy_agent_meta` holds the drain cursor, and `alchemy_agent_schema` holds the schema version.
Alchemy's own job table holds the one pending `drain` callback.

**Creating a task.** `enqueue` runs in one `storage.transaction`. It decodes the input
(an unknown task or invalid input is stored as `failed` right away), inserts the row (an existing
task id is left untouched), and schedules the `drain` callback for now. A write and the scheduling
of its follow-up commit together.

**The drain loop.** A durable callback, single-flight per Durable Object:
1. Any row still `running` belongs to an execution that ended without settling it. It counts as a
   lost attempt (`ExecutionLost`): back to `pending` with a backoff, or `failed` at the cap.
2. Up to 10 due rows, FIFO. For each: mark `running` and increment `attempts` (committed
   before the handler runs), run the handler, then commit one of: `completed` with the result,
   `failed` (a `PermanentError`, an unknown task, invalid input, or the attempt cap), or `pending`
   with `run_at = now + delay × 2^(attempts−1)`.
3. If any row is still pending, reschedule the drain at the earliest `run_at`, passing an
   increasing cursor as `progress`. Without it, Alchemy counts each reschedule as a retry.

The drain never fails because of agent code. Handler failures are recorded in the row, so the
inbox, not Alchemy, decides about retries, and every task settles. Alchemy retries the drain
callback itself only when the execution dies (for example, the CPU limit), and step 1 recovers.

**Callers.** `Agent.lookup(Agent)` in a Worker, `Agent.use(Agent)` in a handler (with `uses`),
and `Api.client(...)` over HTTP all return the same `AgentClient`, typed from the agent
definition. `send` returns a `TaskHandle`, whose `result()` polls the record until it settles.

## To be added after M0

- Components and responsibilities, and how they map onto Cloudflare and Alchemy resources.
- Data flow for: creating a task, the drain loop, send/call/reply, an LLM turn, listener → workflow,
  and human request → resume.
- Storage layout: what is stored per agent, what is global, and what is in the workspace.
- Failure handling: what happens on retry, redeploy, eviction, and duplicate events.
