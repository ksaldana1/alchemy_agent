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
- Run the Alchemy CLI from the repo root with `bun run alchemy …` (or `bun run dev|deploy|plan|destroy`). These run it inside `spikes/` under Bun. A bare `bunx alchemy` at the root fetches a temporary copy that can't load `@effect/platform-bun`.
- Bun uses its isolated linker here, so each workspace package declares every dependency it imports.
- Commands. Library tests: `bun run test` (local workerd) and `bun run test:cloud` (deploys
  `test_$USER`, then destroys it). Spike suites: `bun run test:spikes[:cloud]`. The Hello
  example: `bun run hello:dev|deploy|plan|destroy|check`. It needs `AGENT_API_TOKEN` in
  `examples/hello/.env`, which is gitignored. The support desk (LLM agents on Workers AI):
  `bun run support:deploy|plan|destroy|demo`, with the same token in `examples/support-desk/.env`.
- `bun run dev` / `hello:dev` use the `local_$USER` stage. `alchemy dev` on a deployed stage
  replaces the live Worker with a local one.
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
