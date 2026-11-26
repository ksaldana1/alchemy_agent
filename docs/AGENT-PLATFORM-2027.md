# "Agent platform 2027", built

John Suh's [Agent platform 2027](https://x.com/john_ssuh/status/2124336124172935336) describes an
agent platform as a list of parts: agents as actors, environments, an access-controlled filesystem,
conversations separate from sessions, an inbox, listeners, workflows, and apps. This repository
builds about half of them on Cloudflare Durable Objects and Effect. This page puts the essay next
to the code: what each section became here, where the two differ, and what is still open.

This repository is an architecture sketch to argue with, not a product. The examples are the
evidence. The [refund desk](../examples/refunds) (crash-safe side effects and a person's approval)
and the [newsroom](../examples/newsroom) (listener → workflow → agents → sign-off) exercise most of
what's below.

| Essay | Here | |
|---|---|---|
| [Agents as actors](#agents-as-actors) | One Durable Object per agent; definition, instance, and session are separate | Built |
| [Environments and computers](#environments-and-computers) | Nothing | Not built |
| [Access-controlled filesystem](#access-controlled-filesystem) | The workspace: homes, a shared area, grants, sharing, an audit | Built, not POSIX |
| [Session vs. conversation](#session-vs-conversation) | Sessions and transcripts; no conversation | The opposite choice, mostly |
| [Inbox](#inbox) | Human requests: typed, expiring, bound to a file revision | Built |
| [Listeners and monitors](#listeners-and-monitors) | File and cron listeners, declared at deploy | Partly |
| [Workflows](#workflows) | `effect/workflow`, with agents as steps | Built, but fixed at deploy |
| [Apps](#apps) | Nothing (the demo page is a hand-written one) | Not built |

The theme that runs through every row: **here, everything is declared at deploy time.** Agent
definitions, grants, listeners, and workflows are code, deployed. The essay wants all of them
created and changed by agents, per tenant, while the system runs. It also wants them to be code,
not drag-and-drop. "Code-defined and agent-modified at runtime" means loading generated code into
a running system. That is the largest gap, and the [last section](#the-gap-deploy-time-vs-runtime)
comes back to it.

---

## Agents as actors

> The status quo [...] is to deploy agents using some form of Temporal. I think this will shift
> toward actors that allow each agent harness to have a fully independent runtime.

**Here.** An agent definition is an `Agent.make(type, [rpcs])` contract plus a Layer that implements
it. Each named agent (`RefundDesk/acme`, `Reviewer/senior`) is one Durable Object instance with its
own SQLite inbox, task history, sessions, and home directory
([ARCHITECTURE.md: principles 1–5](ARCHITECTURE.md#principles)). Agents talk only by messages. The
definition / instance / session split the essay asks for is the
[glossary's](GLOSSARY.md#identity-and-execution) first three terms.

**Where it differs: it is actors *and* Temporal.** Inside each actor, a handler is replayed the way a
Temporal workflow is. Calls to other agents go through an outbox and are identified by their
position (`<owner>#c<n>`). A handler that waits (on another agent, a model turn, a person) is
interrupted, and runs again from the top when the answer arrives, getting recorded results back
([internal/client.ts](../packages/agent/src/internal/client.ts),
[the refund desk's handler](../examples/refunds/src/agents.ts)). Actors give the isolation and the
addressing; replay gives crash safety *inside* an actor. Neither alone gets you "the refund was
issued once although the process died after the gateway moved the money". So the essay's
"Temporal → actors" may be better read as "Temporal *inside* actors".

**Where it differs: isolation of state, not of code.** The essay wants actors so that harnesses can
rewrite their own extensions safely. Here the boundary between framework and agent code is
enforced by the API, not by a sandbox ([ARCHITECTURE.md: trust boundary](ARCHITECTURE.md#trust-boundary)),
and every agent type runs in one Worker. Two different agent loops can coexist (a handler is
arbitrary code; `Agent.generate` is one optional loop), but nothing stops one from misbehaving.
Running generated code in an isolate with only the capabilities it is given (Cloudflare's Worker
Loader) is on the [Later list](PLAN.md#later).

**We tried the obvious alternative.** Spike 0.7 asked whether agents should just be `effect/cluster`
entities on Cloudflare. No: its persisted requests can't wait without a heartbeat and a replay on
every wake, and its messages have no outbox that commits with the agent's state
([spike 0.7](spike/0.7-effect-cloudflare-cluster.md)).

**Open questions.**
- Is a Durable Object per agent the right grain, or per session? Today a long-running turn and a
  new task on the same agent share one single-threaded object.
- Self-improving harnesses need code isolation. Is an isolate per agent enough, or does a harness
  that writes its own hooks need a full sandbox (see the next section)?

## Environments and computers

> N agents should be able to work with M computers [...] These environments should be
> configurable agentically.

**Not built.** There are no sandboxes or VMs here; agents act through tools and the workspace. The
natural fit on this architecture is a computer as another actor: a `Sandbox` agent per environment,
with tasks like `Exec` and `Snapshot`, so N agents can call M sandboxes with the same addressing,
outbox, and causation as everything else. Cloudflare Containers or an outside provider would sit
behind it, the way the simulated payment gateway sits behind `Payments`.

**Open question.** Where do the workspace and a computer meet? The essay keeps the filesystem
separate from the computer. A sandbox would need the workspace mounted, with the agent's grants,
which R2 objects behind a permission check (below) can't offer as POSIX.

## Access-controlled filesystem

> Swarms of agents will operate on a shared POSIX filesystem in the cloud, with access controls
> attached to the filesystem separately from the computer.

**Here.** The workspace ([Workspace.ts](../packages/agent/src/Workspace.ts),
[ARCHITECTURE.md: the workspace](ARCHITECTURE.md#the-workspace-implemented-in-m4)):
- Each agent has a **home** (`~/`, really `agents/<type>/<name>/`) where it can do anything.
- The **shared area** (`shared/…`) is reachable only through **grants** the platform declares.
- An agent can **share** a file from its own home with another agent. A share can't be passed on.
- Every decision, allowed or denied, is **audited** with the task and attempt that asked.
- A file has **revisions**. An approval names one, and an approval for an older revision doesn't
  count (the newsroom's sign-off).
- The caller's identity comes from the running task, never from an argument (principle 6).
- LLM agents get the same operations as tools that return a denial to the model instead of failing.

**Where it differs.** It isn't POSIX: logical paths over R2 objects, read and written whole, with no
directories as objects, no `.git`/`jj`, and nothing to mount. The permission check is one Durable
Object in front of the bucket, so it is consistent and auditable but also a single serialization
point for every file operation of every agent.

**Where it differs: who is "who".** The essay lists the configurations organizations will want:
agents like humans, or permission based on who initiated the turn. Here it is only the first: the
agent running the task. But every task carries its **causation** (the task, turn, listener, or
workflow run that created it), so the initiator is recoverable. "This agent may read the
customer's files only while working on a request that customer started" is a policy over the
causation chain that the platform could check. It doesn't yet.

**Open questions.**
- Initiator-based permissions: walk the causation chain on every check, or carry an initiator
  identity with each task?
- Is a shared POSIX filesystem the right abstraction for agents at all, or is it a convenience for
  sandboxes? Whole-file writes with revisions made approvals easy here; POSIX would make them hard.

## Session vs. conversation

> What the user interacts with should be abstracted from the core agent session as a
> "conversation." [...] Direct access to agent sessions should be akin to reading the codebase.

**Here, mostly the opposite.** The glossary defines a **session** as "a persistent conversation
between an LLM agent and whoever is prompting it", and lists "conversation" under *avoid*. The
API streams a session's transcript to clients (`GET /agents/:type/:name/sessions/:id/events`), and
the support desk's chat agent returns its session's text as the reply. That is the status quo the
essay calls wrong.

**But the refund desk already works the essay's way.** The customer never sees RefundDesk's session.
The model's output is typed (`Assessment`), and what reaches the customer is an email sent
through a call to another agent (`Mailer.SendEmail`): content for the user posted through a tool
call, not exposed assistant text. The email thread is the conversation; the session is for
debugging. The difference is that here it's a pattern the app follows, not a concept the platform
has.

**Open questions.**
- Should "conversation" be a platform concept, a durable channel with participants (people and
  agents) that agents post to through a tool, with sessions behind it as implementation detail?
- One conversation, many sessions (several agents in one thread), or one session, many
  conversations (an agent answering in Slack and email)?
- If the transcript is only for debugging, who may read it? It is the most sensitive record an
  agent has.

## Inbox

> Users will increasingly interact with a focused inbox of approvals and questions.

**Here.** Human requests ([Human.ts](../packages/agent/src/Human.ts),
[ARCHITECTURE.md: human requests](ARCHITECTURE.md#human-requests-and-inspection-implemented-in-m7)).
`Human.request(name, { schema, prompt, timeout })` asks a person and waits for a typed answer;
`Human.approval` asks about one revision of a file. A request is a task on a built-in agent, so it
waits like any other call: the asking handler, tool, or workflow holds no execution, for days if
need be, through deploys. It ends answered, expired, or canceled. People see them at
`GET /requests` and answer at `POST /requests/:id/respond`. Each request carries the JSON Schema of
the answer it expects.

That last point is the essay's "tenant-specific apps for reviewing intermediate work" in a small
form: a request says what answer it needs, so a UI can be generated from it. The refund desk's
manager queue is a hand-written one.

**A naming collision.** In this repo, **inbox** means an agent's queue of tasks, not a person's.
What the essay calls an inbox is human requests here.

**Open questions.**
- Requests are platform-wide. A real inbox is per person or role, with routing, delegation, and
  escalation. Is that a platform concept or an app on top?
- Should a request carry the app that reviews it (a reference to a page in the workspace), not
  just a schema?

## Listeners and monitors

> Agents should be able to define streams of events from the external world to listen to on a
> per-tenant basis [...] These listeners should be buildable on the fly.

**Here.** `Listen.files(prefix)` (a file appears in the workspace) and `Listen.cron(expr)`, each
either starting a workflow run or sending a task to an agent
([Listen.ts](../packages/agent/src/Listen.ts),
[ARCHITECTURE.md: listeners](ARCHITECTURE.md#listeners-implemented-in-m6)). Events go through one
queue with retries and a dead-letter log. Duplicate events create nothing new, because the
target's key comes from the event. Translating events "deterministically or agentically" works
already: `.send` to an LLM agent that triages is the agentic filter.

**Where it differs: declared, not built on the fly.** Listeners are in `Platform.make({ listeners })`
and become infrastructure at deploy: an R2 notification rule per prefix, cron triggers on the
Worker. Cloudflare even refuses overlapping prefixes in one configuration. An agent can't add one
at runtime, and there are no WebSocket, Kafka, or CDC sources.

**What runtime listeners would take.** Separate the source from the subscription. Sources stay
infrastructure (one notification rule for the whole bucket, a webhook endpoint, a consumer per
stream). Subscriptions become data: rows an agent writes ("emails from acmecorp.com, for a week,
to this agent"), matched by the listener consumer. The essay's examples are all subscriptions.

**Open question.** Who owns a subscription's lifetime? "For the next week" means listeners need
expiry, and a listener an agent created needs an owner to clean up after.

## Workflows

> Agents should be able to define durable workflows on a per-tenant basis [...] code-defined
> workflows rather than drag-and-drop automations.

**Here.** Workflows are plain `effect/workflow` on Alchemy's Durable Object engine
([publish.ts](../examples/newsroom/src/publish.ts)). Each agent call is a durable step, waiting on
a person or a signal holds nothing, and a duplicate start is the same run. The newsroom's
acceptance test runs three articles through intake, an LLM editor, two reviewers (one denied
access), and a person's sign-off while the editor crashes and every event arrives twice
([newsroom.test.ts](../examples/newsroom/test/newsroom.test.ts)).

**Where it differs.** "Code-defined" yes; "per tenant, defined by agents" no. A workflow is deployed
code. The same is true of agents themselves, which raises a question the essay doesn't address:
when an agent handler can be long-running and replayed, what is the difference between an agent
and a workflow? Here the answer is mostly addressing. An agent has a name and an inbox and serves
many tasks; a workflow run has a key and serves one outcome.

## Apps

> All interfaces should be apps in your filesystem that users can customize.

**Not built.** The refund desk's [page](../examples/refunds/demo/page.ts) is the closest thing: one
HTML file served by the Worker, reading the same HTTP API a script would. If it lived in the
workspace instead of the bundle, an agent with write access could change it, and per-tenant copies
are a `Workspace.copy` away. That is roughly the essay's "each user gets a clone of the codebase",
with the workspace's grants deciding who may edit which app. Serving workspace files as pages is
small; letting those pages call the API safely is the real work.

## The gap: deploy time vs. runtime

Every row above that says "partly" says it for the same reason. Here:
- **Agent definitions** are TypeScript classes bound to Durable Object classes at deploy.
- **Grants** are declared on the platform and replaced on every activation.
- **Listeners** are queue and notification infrastructure.
- **Workflows** are registered with the engine at deploy.

The essay wants each of these created by agents, per tenant, without a deploy, and also wants them
to be real code. Pieces that would get there:
1. **Generated code in isolates.** Worker Loader runs code that isn't in the bundle, with only the
   bindings it is handed. An agent definition becomes source in the workspace, loaded on demand.
2. **Versioned definitions.** A session or a run pinned to the definition version it started on, so
   changing an agent doesn't break work in flight. Durable Objects already make this sharp: after a
   deploy, an object can run old code for minutes.
3. **Grants and subscriptions as data.** Rows an agent with the right grant can write, not
   declarations.
4. **Tenancy.** Everything here is one tenant per deploy ([out of scope](ARCHITECTURE.md#out-of-scope)).
   A tenant would be a namespace over agent names, workspace paths, and listeners.

## What the essay doesn't cover

The essay is about surfaces: what users and agents see and configure. Building it made clear how
much sits under them, and how much of it comes down to the semantics of a crash:
- **Exactly-once side effects** need the task id as an idempotency key, an outbox that commits with
  state, and handlers that replay instead of restart. Agents that "operate the business" will
  issue refunds and send emails; the [refund desk](../examples/refunds) is about nothing else.
- **Waiting must cost nothing.** Approvals take days and listeners fire rarely. Anything that holds
  a process or a heartbeat while waiting won't scale to the inbox the essay describes.
- **Causation** (which task, turn, listener, or run created this work) is what makes a swarm
  debuggable, and is the raw material for initiator-based permissions.
- **Typed contracts** between agents, people, and workflows (`Rpc` schemas, request schemas) are
  what let apps and inboxes be generated rather than hand-written.
