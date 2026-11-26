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

## Project docs

- `docs/PLAN.md`: milestones and design decisions D1–D9.
- `docs/GLOSSARY.md`: the project's terms. Use them in code, docs, and conversation.
- `docs/ARCHITECTURE.md`: principles and invariants that every change must keep.
- `docs/spike/`: the M0 spikes. Record findings in each spike doc.
- `~/projects/mini-agent-runtime`: the behavioral reference. Its tests describe behavior to
  carry over; write fresh code here rather than porting its code.
