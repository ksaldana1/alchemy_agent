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

## To be added after M0

- Components and responsibilities, and how they map onto Cloudflare and Alchemy resources.
- Data flow for: creating a task, the drain loop, send/call/reply, an LLM turn, listener → workflow,
  and human request → resume.
- Storage layout: what is stored per agent, what is global, and what is in the workspace.
- Failure handling: what happens on retry, redeploy, eviction, and duplicate events.
