# alchemy-agent

A framework for building systems of persistent, cooperating agents. Agents have durable
identities and inboxes. They exchange messages, work in permissioned workspaces, react to
events, and take part in long-running workflows that people can step into.

## Language

### Identity and execution

**Agent definition**:
The code that describes a kind of agent: its name, its tasks, and (for LLM agents) its
instructions, model, and tools.
_Avoid_: Agent class, agent type, agent module

**Agent**:
One persistent identity created from an agent definition and addressed by its definition and a
name. It exists as soon as it is addressed, and it outlives every execution.
_Avoid_: Instance, actor, worker

**Agent name**:
The name that picks out one agent within its definition (for example, `junior` within
`Reviewer`). Same name, same agent: same inbox, same workspace, same history.
_Avoid_: Agent id, key

**Execution**:
A period during which an agent's code is actually running. Executions are temporary, are
started and stopped by the platform, and are never addressed or relied on by agent code.
_Avoid_: Runtime, incarnation, process, instance

**Platform**:
Everything the framework owns and runs on the agents' behalf: inboxes, delivery, workspaces,
listeners, workflows, and the API. Agent code depends on the platform. The platform never depends
on agent code being running.
_Avoid_: Kernel, host, server

### Work and delivery

**Task**:
One durable request for an agent to do a named piece of work with a given input. A task may be
delivered more than once, but it is recorded once.
_Avoid_: Job, message, request, work item

**Task id**:
The identifier of a task, chosen by or derived from whoever creates it. Creating a task with an
id that already exists has no effect, so the task id doubles as the idempotency key.
_Avoid_: Idempotency key (when referring to tasks), message id

**Inbox**:
The ordered record of every task addressed to an agent, with each task's status and outcome.
The platform owns it, not the agent's code.
_Avoid_: Queue, mailbox

**Attempt**:
One delivery of a task to the agent's code. A task that fails in a retryable way, or whose
execution is lost partway through, gets another attempt with the same task id.
_Avoid_: Retry (for the delivery itself), try

**Settled**:
The state of a task that has finished for good, either completed with a result or failed. A
settled task never runs again.
_Avoid_: Done, finished, resolved

**Permanent failure**:
A failure that settles a task right away, without further attempts. Examples: invalid input, an
unknown task name, a denied permission, or an error the agent marks as permanent.
_Avoid_: Fatal error, non-retryable error

**Task handle**:
What a caller holds after creating a task: its task id, plus the means to check its status and
await its result.
_Avoid_: Future, promise, receipt

### Agents working together

**Send**:
An agent creating a task in another agent's inbox and carrying on without waiting for the
result.
_Avoid_: Fire-and-forget, emit, post, dispatch

**Call**:
An agent or workflow creating a task in another agent's inbox and waiting for its result.
_Avoid_: Request, invoke, ask, delegate

**Reply**:
The result of a call, delivered back to the waiting caller.
_Avoid_: Response, callback, answer

**Waiting**:
The state of a task, session, or workflow run that has paused until something arrives: a reply,
a person's response, a signal, or a time. Waiting work holds no execution.
_Avoid_: Blocked, suspended, sleeping, parked

**Causation**:
The link from a task back to the task, turn, listener, or workflow run that created it. Following
causation reconstructs a chain of work across agents.
_Avoid_: Parent, trace, correlation (when meaning the direct cause)

### LLM agents

**Session**:
A persistent conversation between an LLM agent and whoever is prompting it. It spans many turns
and outlives every execution.
_Avoid_: Chat, thread, conversation, context

**Turn**:
The work an LLM agent does in a session in response to one prompt, from receiving the prompt to
producing a final answer.
_Avoid_: Run, round, exchange, request

**Step**:
One recorded unit of a turn: a single model call or a single tool call. A turn resumes from its
last recorded step and never starts over.
_Avoid_: Iteration, hop, action

**Transcript**:
The ordered, durable record of every step in a session. It is the session's memory and the point
the session resumes from.
_Avoid_: History, messages, log, context window

**Tool**:
A typed capability an LLM agent can choose to use during a turn, such as reading a file or
calling another agent.
_Avoid_: Function, action, skill, plugin

**Model**:
The language model an LLM agent uses for its model calls, chosen per agent definition.
_Avoid_: LLM provider, engine, brain

### Workspace

**Workspace**:
The permissioned file space agents read and write. It holds every agent's home area plus the
shared area.
_Avoid_: Filesystem, storage, bucket, drive

**Home**:
The part of the workspace that belongs to one agent, written `~/` from that agent's point of
view. Its owner always has full access.
_Avoid_: Private directory, agent folder

**Shared area**:
The part of the workspace that belongs to the platform (`shared/…`). Access to it is granted only
by the platform, never by agents.
_Avoid_: Public directory, common area

**Logical path**:
A workspace path as agents write it (`~/notes.md`, `shared/documents/x`). Agents never see where
or how a file is actually stored.
_Avoid_: Key, storage path, URL

**Grant**:
Permission for an agent to read or write a file or everything under a path. When grants overlap,
the most specific one applies.
_Avoid_: ACL, permission entry, rule

**Share**:
An agent giving another agent a grant on something in its own home. A shared grant cannot be
passed on further.
_Avoid_: Delegate access, publish

**Revision**:
One specific version of a file in the workspace. Approvals and reviews refer to a revision, not to
a path.
_Avoid_: Version (alone), snapshot, etag

### Events and orchestration

**Listener**:
A declared rule saying that when something happens (a file arrives under a path, or a schedule
fires), a task is created or a workflow run is started. Listeners keep working while the target
agents are idle.
_Avoid_: Trigger, watcher, hook, subscription

**Workflow**:
Code that coordinates calls to several agents, timers, and human requests toward one outcome,
and survives any interruption. Defined once and run many times.
_Avoid_: Pipeline, flow, job, orchestration

**Workflow run**:
One execution of a workflow with a specific input, with its own durable progress and result.
_Avoid_: Instance, execution, job

**Signal**:
A named, typed value sent to a waiting workflow run from outside it, such as an approval.
_Avoid_: Event (alone), message, notification

### People

**Human request**:
A durable question for a person, expecting a typed response and bound to the context it is
about. It ends up resolved, expired, or canceled.
_Avoid_: Prompt, ask, ticket, notification

**Approval**:
A human request whose response is yes or no, bound to one revision. An approval of an older
revision does not count for a newer one.
_Avoid_: Sign-off, confirmation
