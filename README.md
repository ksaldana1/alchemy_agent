# alchemy-agent

Build persistent agents on Cloudflare with [Effect](https://effect.website) and
[Alchemy](https://alchemy.run). Define typed tasks, implement handlers, and call agents by name.
The library manages durable inboxes, retries, and communication between agents.

Use it for LLM assistants with tools and conversation history, agents that work together on
shared files, or workflows that wait for human approval. Agents can also run ordinary TypeScript
logic without a model.

```ts
const greeters = yield* Greeter.client;
const { greeting } = yield* greeters("ada").Greet({ name: "Ada" });
```

`"ada"` selects an agent with its own inbox and history. `Greet` and its input and result types
come from your contract. This call works inside an Effect program in another agent, a workflow,
or a script connected over HTTP.

> **Pre-release:** the package is not published to npm, and the API may change. For now, use it
> within this repository's Bun workspace. It pins `effect` to `4.1.5` and `alchemy` and
> `@alchemy.run/cloudflare` to `2.0.0-beta.87`.

- [Try the Hello example](#try-the-hello-example)
- [Build your own agent](#build-your-own-agent)
- [Call it from TypeScript](#call-it-from-typescript)
- [How durability works](#how-durability-works)
- [Explore the library](#explore-the-library)

## Try the Hello example

You'll need [Bun](https://bun.sh), a checkout of this repository, and a Cloudflare account
connected to Alchemy. The Hello example does not need an LLM or a model API key.

Run these commands from the repository root:

```sh
bun install
bun run hello:alchemy profile
```

The second command opens Alchemy's authentication setup. See the
[Cloudflare setup guide](https://alchemy.run/cloudflare/setup) for account setup details.

Create `examples/hello/.env` with a long random token of your choosing (this file is gitignored):

```dotenv
AGENT_API_TOKEN=replace-with-your-long-random-token
```

Deploy the example:

```sh
bun run hello:deploy --yes
```

This creates Cloudflare resources on the `dev_$USER` stage. Copy the `url` from the deployment
output, then set these variables in your terminal. Use the same token you put in `.env`;
`curl` does not load that file for you.

```sh
export URL="https://your-worker.workers.dev"
export AGENT_API_TOKEN="replace-with-your-long-random-token"

curl -X POST "$URL/agents/Greeter/ada/tasks" \
  -H "authorization: Bearer $AGENT_API_TOKEN" \
  -H "content-type: application/json" \
  -d '{ "tag": "Greet", "payload": { "name": "Ada" } }'
```

The API returns `202` with a `taskId`. Use that id to read the result:

```sh
export TASK_ID="paste-the-returned-task-id"
curl "$URL/agents/Greeter/ada/tasks/$TASK_ID?wait=1" \
  -H "authorization: Bearer $AGENT_API_TOKEN"
```

When completed, the response contains:

```json
{
  "status": "completed",
  "result": { "greeting": "Hello, Ada!", "by": "Greeter/ada" }
}
```

The response also includes task metadata. `wait=1` waits up to 20 seconds; if the task is still
pending or running, repeat the request.

For local development, run `bun run hello:dev` and use the URL it prints. It uses a separate
`local_$USER` stage. To remove the deployed example and its stored data when you're finished:

```sh
bun run hello:destroy --yes
```

Read [the Hello agent](examples/hello/src/greeter.ts) for more tasks, including retries and
calling another agent. Hello shows how to mount the API in a custom Worker; the next section
uses `Platform.make` to set up the Worker for you.

## Build your own agent

The examples assume some familiarity with Effect: `Effect.gen` composes operations, `yield*`
runs an Effect within that program, and a `Layer` provides services an implementation needs.

Create an app under `examples/my-agent/` so it belongs to this repository's workspace. Add this
`package.json`, then run `bun install` from the repository root:

```json
{
  "name": "my-agent",
  "private": true,
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

The `workspace:*` dependency resolves the local library; it will not work in an unrelated project.
The following file paths are relative to `examples/my-agent/`.

An app has four parts: **contracts** (what agents accept and return), **implementations**
(Layers), a **platform** (one Worker that hosts it all), and an Alchemy **stack**.

### 1. Define a contract

Contracts describe task inputs and outputs with Effect `Rpc`s. Other agents and scripts import
the contract to get a typed client without importing the handler implementation.

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

### 2. Implement the handler

A Layer supplies the implementation of your contract:

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

### 3. Host the agent

The platform lives in its own module, which the Worker bundles (`main`):

```ts
// src/platform.ts
import { Platform } from "alchemy-agent";
import { GreeterLive } from "./agents.ts";

export default Platform.make("MyAgentApi", {
  main: import.meta.url,
  agents: [GreeterLive],
});
```

### 4. Declare the stack

```ts
// alchemy.run.ts
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import MyAgent from "./src/platform.ts";

export default Alchemy.Stack(
  "MyAgent",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const app = yield* MyAgent;
    return { url: app.url };
  }),
);
```

Create `examples/my-agent/.env` with `AGENT_API_TOKEN`, as in the Hello example. From the
repository root, deploy your app with:

```sh
bun --cwd examples/my-agent --bun alchemy deploy --stage dev_$USER --yes
```

Use the same HTTP requests as above with your new deployment URL. This handler returns
`{ "greeting": "Hello, Ada!" }` as its result.

## Call it from TypeScript

For scripts and tests, connect to the deployment and use the same typed agent client.
Save this as `examples/my-agent/src/client.ts`:

```ts
import { Platform } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { Greeter } from "./contracts.ts";

const url = process.env.URL;
const token = process.env.AGENT_API_TOKEN;
if (!url || !token) throw new Error("Set URL and AGENT_API_TOKEN before running this script");

const program = Effect.gen(function* () {
  const greeters = yield* Greeter.client;
  const result = yield* greeters("ada").Greet({ name: "Ada" });
  console.log(result.greeting);
});

await Effect.runPromise(
  program.pipe(
    Effect.provide(Platform.connect({ url, token })),
    Effect.provide(FetchHttpClient.layer),
  ),
);
```

With `URL` and `AGENT_API_TOKEN` set in your terminal, run it from the repository root:

```sh
bun --cwd examples/my-agent src/client.ts
# Hello, Ada!
```

Inside a hosted agent or workflow, the platform provides the client connection for you.
See [calling agents](docs/GUIDE.md#calling-agents) for sending without waiting and handling errors.

## How durability works

An **agent definition** describes the tasks an agent accepts. An **agent** is one named identity
of that definition: `Greeter/ada` and `Greeter/bob` each have their own inbox and history.
A **task** is a durable request to one of those agents.

Tasks are delivered at least once and retried after retryable failures. A task id identifies
one task: resending it with the same payload returns the existing task; changing the payload
fails with `InputConflict`. Use an Rpc's `primaryKey` to derive stable task ids from your input.

When a handler waits for another agent, a model turn, or a human response, its execution ends.
When the answer arrives, the handler runs again and recorded calls return their stored results.
Scripts calling over HTTP stay running and poll for the result.

**Handler code can run more than once.** Keep durable calls in the same order on each run, and
use stable idempotency keys for external side effects such as charging a payment or sending an
email. Completed model and tool steps are checkpointed; ordinary local variables and arbitrary
side effects are not. Read the [execution rules](docs/GUIDE.md#rules-to-know) before adding side effects.

## Explore the library

| I want to… | Start here |
|---|---|
| Define tasks, errors, retries, and cancellation | [Agents](docs/GUIDE.md#agents) |
| Add a model, tools, or conversation history | [LLM agents](docs/GUIDE.md#llm-agents) · [Support desk example](examples/support-desk) |
| Let agents read and share files | [The workspace](docs/GUIDE.md#the-workspace) |
| Coordinate agents and durable timers | [Workflows](docs/GUIDE.md#workflows) |
| Wait for a person's decision | [Human requests](docs/GUIDE.md#human-requests) |
| Start work from an upload or schedule | [Listeners](docs/GUIDE.md#listeners) |
| Integrate with another application | [HTTP API](docs/GUIDE.md#the-platform-and-its-http-api) · [Script clients](docs/GUIDE.md#from-scripts) |
| Test with scripted models and simulated crashes | [Testing](docs/GUIDE.md#testing) |
| See the pieces working together | [Newsroom example](examples/newsroom) · [Acceptance test](examples/newsroom/test/newsroom.test.ts) |

For the full walkthrough, see the [library guide](docs/GUIDE.md). For project terminology and
internal design, see the [glossary](docs/GLOSSARY.md), [architecture](docs/ARCHITECTURE.md),
and [milestone plan](docs/PLAN.md).
