// Listeners (M6): rules that create work when something happens, while the agents are idle.
// Declared on the platform: `Platform.make({ …, listeners: [...] })`.
//
//   // A run per uploaded file. The run's key comes from the workflow's idempotencyKey, so a
//   // duplicate event (or a replay) starts nothing new.
//   Listen.files("shared/submissions/", { suffix: ".txt" }).start(PublishArticle, (file) => ({ path: file.path, name: file.basename }))
//
//   // A task per cron fire. The task id is the Rpc's primaryKey, else `listener/<name>/<scheduledTime>`.
//   Listen.cron("0 * * * *").send(Digest, "daily", "Summarize", (fire) => ({ at: fire.scheduledTime }))
//
// Each event goes through one queue with retries; one that keeps failing (a mapping that throws,
// a payload that doesn't decode) is dead-lettered, logged as `listener.error`, and recorded
// (`GET /listeners/log`).
import type { Rpc } from "effect/rpc";
import type { Workflow } from "effect/workflow";
import type { CronFire, FileEvent, Listener, Target } from "./internal/listeners.ts";

export type { CronFire, FileEvent, Listener };

type PayloadOf<W> = W extends Workflow.Workflow<any, infer P, any, any> ? P["Type"] : never;
type AgentLike<Rpcs extends Rpc.Any> = { readonly type: string; readonly protocol: ReadonlyArray<Rpcs> };

/** What a source's events can do: start a workflow run, or create an agent task. */
export interface Source<E> {
  /** Starts a run per event. Duplicate events map to the same run through the workflow's idempotencyKey. */
  start<W extends Workflow.Any>(workflow: W, make: (event: E) => PayloadOf<W>): Listener;
  /** Creates a task per event on agent `name`. */
  send<Rpcs extends Rpc.Any, const Tag extends Rpc.Tag<Rpcs>>(
    agent: AgentLike<Rpcs>,
    name: string,
    task: Tag,
    make: (event: E) => Rpc.PayloadConstructor<Extract<Rpcs, { readonly _tag: Tag }>>,
  ): Listener;
}

const source = <E>(name: string, build: (target: Target<E>) => Listener): Source<E> => ({
  start: (workflow, make) => build({ kind: "workflow", workflow, make: make as (e: E) => unknown }),
  send: (agent, agentName, task, make) => build({ kind: "agent", agent, name: agentName, tag: task, make: make as (e: E) => unknown }),
});

/**
 * Files written under `prefix` in the workspace (logical paths, e.g. `shared/submissions/`).
 * Only the platform and agents with grants can write there, so a listener sees what they write.
 */
export const files = (prefix: string, options?: { readonly suffix?: string; readonly name?: string }): Source<FileEvent> => {
  const name = options?.name ?? `files:${prefix}${options?.suffix ?? ""}`;
  return source(name, (target) => ({ name, source: { kind: "files", prefix, suffix: options?.suffix }, target }) as Listener);
};

/** A cron schedule (Cloudflare Cron Triggers syntax, UTC). */
export const cron = (expression: string, options?: { readonly name?: string }): Source<CronFire> => {
  const name = options?.name ?? `cron:${expression}`;
  return source(name, (target) => ({ name, source: { kind: "cron", cron: expression }, target }) as Listener);
};
