# alchemy-agent

## Alchemy reference

When writing or changing Alchemy code (stacks, Workers, Durable Objects, bindings, durable
callbacks, workflows, queues, the `alchemy` CLI, `@alchemy.run/test`), first read the relevant
page from **https://alchemy.run/llms.txt**. It's an index of the doc pages;
https://alchemy.run/llms-full.txt has all of them in one file.

The docs track Alchemy's latest release, but this repo pins `2.0.0-beta.87`. When the docs and
the installed type definitions in `node_modules` disagree, the installed types are what this repo
runs. Record the difference in `docs/spike/0.6-verified-facts.md`.

## Gotchas

- In Effect 4.1, the modules are `effect/ai`, `effect/http`, and `effect/workflow`. Older docs and examples use `effect/unstable/*`.
- Run the Alchemy CLI from the repo root with `bun run alchemy …` (or `bun run dev|deploy|plan|destroy`). The CLI's own default stage for `deploy`/`plan`/`destroy` is `live_$USER`. The scripts pass `--stage dev_$USER`, so add `--stage dev_$USER` yourself when you run `bun run alchemy deploy` directly. Non-interactive deploys need `--yes`. These run it inside `spikes/` under Bun. A bare `bunx alchemy` at the root fetches a temporary copy that can't load `@effect/platform-bun`.
- Bun uses its isolated linker here, so each workspace package declares every dependency it imports.
- Commands. Library tests: `bun run test` (local workerd) and `bun run test:cloud` (deploys
  `test_$USER`, then destroys it). Spike suites: `bun run test:spikes[:cloud]`. The Hello
  example: `bun run hello:dev|deploy|plan|destroy|check`. It needs `AGENT_API_TOKEN` in
  `examples/hello/.env`, which is gitignored. The support desk (LLM agents on Workers AI):
  `bun run support:deploy|plan|destroy|demo`, with the same token in `examples/support-desk/.env`.
  `support:demo <url> workspace` is M4's deployed check (the workspace and its audit). The
  newsroom (M5 workflows, M6 listeners): `bun run newsroom:deploy|destroy|demo`, token in `examples/newsroom/.env`;
  `newsroom:demo <url> submit|poison` is M6's deployed check; `publish`, `approve`, and `stale` are M7's.
  M8's acceptance test: `bun run newsroom:test[:cloud]` (scripted model, chaos). The end-to-end run on a
  stage: `bun run newsroom:run <url>`.
- `bun run dev` / `hello:dev` use the `local_$USER` stage. `alchemy dev` on a deployed stage
  replaces the live Worker with a local one.
- The public API follows `effect/cluster`'s `Entity`: `Rpc` contracts in `Agent.make(type, [rpcs])`,
  `X.toLayer(X.of({ … }), { retry, toolkit, limits })`, `(yield* X.client)(name).Task(payload)`,
  and one `Agent.generate({ prompt, schema? })`. `docs/NORTH-STAR.md` shows the target. New API
  should look like Effect's, not like a framework of its own.
- Inside an agent (handler or tool), client calls go through the outbox and are identified by
  their position (`<owner>#c<n>`). A handler must make the same calls in the same order on every
  run, and code before a waiting call can run more than once. A waiting call or
  `Agent.generate` suspends by interrupting itself (as `effect/workflow` does); once it has, the
  execution's outcome is discarded, so code that catches the interruption (`catchCause`, `exit`)
  runs on for nothing. Framework code suspends only through `suspend()` in `internal/core.ts`.
- Workflows are plain `effect/workflow` on Alchemy's Durable Object engine (D9). `Workflow.make(name, options)`
  takes the name first in Effect 4.1. Call agents from the workflow body, never inside an `Activity`.
  Host agents and workflows together with `Platform.layer` / `Platform.make`, which gives the engine
  to the agents (otherwise their replies can't reach a waiting run).
- Stacks keep state with `Cloudflare.state()` (in Cloudflare, not `.alchemy/`), and the local test run
  (`ALCHEMY_TEST_DEV=1`) uses the same `test_$USER` stage name. A resource that fails to create can leave a
  record that blocks the next run; remove it with `bun --bun alchemy state rm -c <stack file> <Stack>/<stage>/<Resource>`
  (records only, never cloud resources).
- Local R2 sends no event notifications. Listener tests put events on the queue with `POST /listeners/events`.
- No formatter is configured. Lines go up to about 140 columns. Don't run `prettier` with its
  defaults (80 columns).
- A class-style `Context.Service` has no `.asEffect()`. Use `yield* Service` or
  `Effect.service(Service)`.
- TypeScript 7 can hide later errors in a file while earlier ones are present. Check each
  `@ts-expect-error` case on its own.
- After a deploy, a Durable Object can run the previous code for a few minutes. Only add RPC
  methods, never remove or change them, and make callers fall back if a new one is missing.
- Deployed Workers freeze `Date.now()` and `performance.now()` during synchronous code. Never
  write a busy loop or a timeout that depends on the clock.

## Project docs

- `docs/NORTH-STAR.md`: the end goal, the newsroom as it should read once M8 is done. Check API
  decisions against it.
- `docs/PLAN.md`: milestones and design decisions D1–D9.
- `docs/GLOSSARY.md`: the project's terms. Use them in code, docs, and conversation.
- `docs/ARCHITECTURE.md`: principles and invariants that every change must keep.
- `docs/spike/`: the M0 spikes. Record findings in each spike doc.
- `~/projects/mini-agent-runtime`: the behavioral reference. Its tests describe behavior to
  carry over; write fresh code here rather than porting its code.

## Vendored Repositories

This project vendors external repositories under @repos/

- Use vendored repositories as read-only reference material when working with related libraries
- Prefer examples and patterns from the vendored source code over generated guesses or web search results
- Do not edit files under @repos/ unless explicitly asked
- Do not import from @repos/ - application code should continue importing from normal package dependencies
When writing Effect code, inspect @repos/effect/ for examples of idiomatic usage, tests, module structure, and API design. Treat it as the source of truth for Effect patterns.
