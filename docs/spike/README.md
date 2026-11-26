# M0 spikes

Milestone 0 in [PLAN.md](../PLAN.md) is a set of spikes. Each one answers a question that
the framework design (decisions D1–D9) depends on, before any library code exists. Every spike
has its own doc here. **When all of them are done, stop and revisit D1–D9.**

| Doc | Question it answers | Decisions it informs | Size |
|---|---|---|---|
| [0.1 Scaffold](0.1-scaffold.md) | Can we build, run locally, deploy, and redeploy an Alchemy v2 Worker + Durable Object with Bun, using pinned versions? | all | S |
| [0.2 Agent typing](0.2-agent-typing.md) | Can `Agent.make` wrap `Cloudflare.DurableObject` and still infer task names, inputs, and outputs end to end? | D1, D2 | M |
| [0.3 Durable callbacks](0.3-durable-callbacks.md) | Is `Alchemy.makeCallback` a good enough scheduler for the inbox drain loop and outbox? | D3, D4, D6, D7 | M |
| [0.4 Workflow engines](0.4-workflow-engines.md) | Native Cloudflare Workflows or `effect/workflow` on the Durable Object engine? | D9 | M |
| [0.5 LLM](0.5-llm.md) | Can we run a single, checkpointable LLM step with tools inside a DO, through Workers AI and AI Gateway, and swap in a scripted model for tests? | D5, D6 | M |
| [0.6 Verified facts](0.6-verified-facts.md) | What are the platform limits and guarantees the design relies on? | all | S |
| [0.7 Effect's Cloudflare cluster](0.7-effect-cloudflare-cluster.md) | Should agents be `effect/cluster` entities on `@effect/platform-cloudflare`, with our waiting layer on top? (added after M8) | D2, D3, D4, D7 | M |

## Order

```
0.1 ──┬── 0.2 ─────────────┐
      ├── 0.3 ── 0.4 ──────┼── review D1–D9
      └── 0.5 ─────────────┘
0.6 is filled in throughout, as each spike confirms or disproves a fact.
```

0.4 comes after 0.3 because both workflow engines need a DO that signals the workflow when its
work finishes, and that DO uses the callback pattern from 0.3.

**Changed 2026-11-25:** after 0.1–0.3, M1 started without waiting for the rest. 0.5 now runs
before M2 and 0.4 before M5, against the real inbox instead of probe DOs. See PLAN.md, M0
status.

## Conventions for every spike

- **Spike code is throwaway.** It lives in `spikes/`, not in `packages/agent/`. There is one
  Alchemy stack (`spikes/alchemy.run.ts`, stack name `Spikes`), and each spike adds its own Durable
  Objects and route prefix (`/counter`, `/echo`, `/probe`, `/wf`, `/llm`). Only the findings and
  the type-level design carry over into M1.
- **Stages.** Local work uses `alchemy dev`. Deployed checks use `--stage dev_$USER`. Cloud test
  suites use the harness default `test_$USER`.
- **Evidence over memory.** Each claim in a "Findings" section links to a doc page, a log line,
  or a test. Write "unverified" when that's all you have.
- **Timebox.** Each doc gives a timebox. If you hit it, write down what's blocking you, pick the
  fallback the doc names, and move on.
- **Fill in the Findings section** at the bottom of each doc. Copy anything that changes a
  decision into the table in [0.6](0.6-verified-facts.md) and into the review notes.

## Output of M0

1. Every spike doc has its Findings filled in.
2. [0.6-verified-facts.md](0.6-verified-facts.md) is complete.
3. `PLAN.md` decisions D1–D9 are updated: confirmed, changed, or marked as risks.
4. Exact pinned versions are recorded in 0.1's findings and in `package.json`.
