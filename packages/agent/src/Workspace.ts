// The workspace: permissioned files for agents (M4).
//
//   const text = yield* Workspace.read("shared/desk/a.md")        // checked against the agent's grants
//   const ref  = yield* Workspace.write("~/summary.md", text)      // ~/ is the agent's home
//   yield* Workspace.share(ref.path, Reviewer.address("senior"), "read")
//
// Paths are logical: `~/x`, `agents/<type>/<name>/x`, `shared/…`. Who is asking always comes from
// the running task, never from an argument. A denial is `FsDenied`, which never retries: a task
// that doesn't declare it fails with `AgentError(reason: FsDenied)`.
//
// Hosting: provide `Workspace.layer({ grants })` in the Worker that hosts the agents. It declares
// the workspace's Durable Object and its R2 bucket; agent code never names either.
import * as Cloudflare from "@alchemy.run/cloudflare";
import { resolveDurableObjectNamespace } from "alchemy/Workers/DurableObject";
import * as Tool from "effect/ai/Tool";
import * as Toolkit_ from "effect/ai/Toolkit";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import { AgentContext, AgentError, Address, FsDenied, Unreachable } from "./internal/core.ts";
import type { Access } from "./internal/paths.ts";
import {
  WORKSPACE_NAME,
  WORKSPACE_TYPE,
  workspaceObject,
  type AuditEntry,
  type Grant,
  type RequestContext,
  type Result,
  type WorkspaceRpc,
} from "./internal/workspace.ts";

export { FsDenied };
export type { Access, AuditEntry, Grant };

/** The file doesn't exist (yet). Not permanent: a retry may find it. */
export class FileNotFound extends Schema.TaggedError<FileNotFound>()("FileNotFound", { path: Schema.String }) {}

/** A file as of one revision. `revision` changes with every write; approvals refer to it. */
export const FileRef = Schema.Struct({ path: Schema.String, revision: Schema.String, size: Schema.Number });
export type FileRef = typeof FileRef.Type;

export const AccessSchema = Schema.Literals(["read", "write"]);

// ---------------------------------------------------------------------------
// Hosting
// ---------------------------------------------------------------------------

/** Hosting the workspace: what `Workspace.layer` produces. */
export interface WorkspaceHost {
  readonly _WorkspaceHost: unique symbol;
}

export interface LayerOptions {
  /** Platform grants: the only way into `shared/`. Seeded on every activation, replacing the last declaration. */
  readonly grants?: ReadonlyArray<Grant>;
  /** Empty the bucket when the stack is destroyed. Default false: destroying a non-empty bucket fails. */
  readonly forceDestroy?: boolean;
}

/**
 * The workspace, hosted in the Worker that provides this Layer (next to the agents): one Durable
 * Object holding grants and the audit, and an R2 bucket holding the files.
 */
export const layer = (options?: LayerOptions): Layer.Layer<WorkspaceHost, never, Cloudflare.Worker> => {
  const Declaration = Cloudflare.DurableObject<WorkspaceHost, WorkspaceRpc>()(WORKSPACE_TYPE);
  return Declaration.make(
    workspaceObject({ grants: options?.grants ?? [], forceDestroy: options?.forceDestroy }).pipe(
      Effect.provide(Cloudflare.R2.ReadWriteBucketBinding),
    ),
  ) as never;
};

// ---------------------------------------------------------------------------
// Reaching the workspace
// ---------------------------------------------------------------------------

const workspaceAddress: Address = { type: WORKSPACE_TYPE, name: WORKSPACE_NAME };
const unreachable = (message: string) => new AgentError({ agent: workspaceAddress, reason: new Unreachable({ message }) });

/** Every operation is safe to retry (writes overwrite, shares upsert), so a failed RPC is retried. */
const callWorkspace = <A>(call: (stub: WorkspaceRpc) => Effect.Effect<A, never, any>): Effect.Effect<A, AgentError> =>
  Effect.flatMap(resolveDurableObjectNamespace<{ getByName: (name: string) => unknown }>(WORKSPACE_TYPE), (ns) =>
    ns === undefined
      ? Effect.fail(unreachable("the workspace isn't hosted here: provide Workspace.layer() in this Worker"))
      : (call(Cloudflare.makeRpcStub<WorkspaceRpc>(ns.getByName(WORKSPACE_NAME))) as unknown as Effect.Effect<A, unknown>).pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause) ? Effect.failCause(cause as Cause.Cause<never>) : Effect.fail(unreachable(String(Cause.squash(cause)))),
          ),
          Effect.retry({ schedule: Schedule.exponential("200 millis"), times: 6 }),
        ),
  ) as Effect.Effect<A, AgentError>;

const unwrap = <A>(result: Result<A>): Effect.Effect<A, FsDenied | FileNotFound> =>
  result.ok
    ? Effect.succeed(result.value)
    : result.error._tag === "FsDenied"
      ? Effect.fail(new FsDenied({ op: result.error.op, path: result.error.path, message: result.error.message }))
      : Effect.fail(new FileNotFound({ path: result.error.path }));

/** The caller, as framework code knows it: the running task's agent. */
const context: Effect.Effect<RequestContext, never, AgentContext> = Effect.map(Effect.service(AgentContext), (ctx) => ({
  caller: ctx.agent,
  taskId: ctx.taskId,
  attempt: ctx.attempt,
}));

const run = <A>(call: (stub: WorkspaceRpc, ctx: RequestContext) => Effect.Effect<Result<A>, never, any>) =>
  Effect.flatMap(context, (ctx) => Effect.flatMap(callWorkspace((stub) => call(stub, ctx)), unwrap));

// ---------------------------------------------------------------------------
// Operations (inside an agent: a handler, or a tool during a turn)
// ---------------------------------------------------------------------------

/** A file's text. */
export const read = (path: string): Effect.Effect<string, FsDenied | FileNotFound | AgentError, AgentContext> =>
  Effect.map(
    run((stub, ctx) => stub.read(ctx, path)),
    (r) => r.text,
  ) as never;

/** Writes (replaces) a file; returns its new revision. */
export const write = (path: string, text: string): Effect.Effect<FileRef, FsDenied | AgentError, AgentContext> =>
  run((stub, ctx) => stub.write(ctx, path, text)) as never;

/** The names directly under a directory; subdirectories end with `/`. */
export const list = (path: string): Effect.Effect<ReadonlyArray<string>, FsDenied | AgentError, AgentContext> =>
  run((stub, ctx) => stub.list(ctx, path)) as never;

/** A file's current path, revision, and size. */
export const stat = (path: string): Effect.Effect<FileRef, FsDenied | FileNotFound | AgentError, AgentContext> =>
  run((stub, ctx) => stub.stat(ctx, path));

/** A file's current revision. */
export const revision = (path: string): Effect.Effect<string, FsDenied | FileNotFound | AgentError, AgentContext> =>
  Effect.map(stat(path), (ref) => ref.revision);

/** Copies a file (read access to `from`, write access to `to`). */
export const copy = (from: string, to: string): Effect.Effect<FileRef, FsDenied | FileNotFound | AgentError, AgentContext> =>
  run((stub, ctx) => stub.copy(ctx, from, to));

/** Gives another agent access to something in this agent's home. Shares can't be passed on. */
export const share = (path: string, to: Address, access: Access = "read"): Effect.Effect<void, FsDenied | AgentError, AgentContext> =>
  run((stub, ctx) => stub.share(ctx, path, { type: to.type, name: to.name }, access)) as never;

/** Takes back a share. */
export const revoke = (path: string, from: Address): Effect.Effect<void, FsDenied | AgentError, AgentContext> =>
  run((stub, ctx) => stub.revoke(ctx, path, { type: from.type, name: from.name })) as never;

// ---------------------------------------------------------------------------
// Platform access (the HTTP API): no agent, so full access and no "~"
// ---------------------------------------------------------------------------

const platform: RequestContext = { caller: null, taskId: null, attempt: null };

/** @internal Used by `Api.routes`. */
export const platformOps = {
  read: (path: string) => Effect.flatMap(callWorkspace((stub) => stub.read(platform, path)), unwrap),
  write: (path: string, text: string) => Effect.flatMap(callWorkspace((stub) => stub.write(platform, path, text)), unwrap),
  list: (path: string) => Effect.flatMap(callWorkspace((stub) => stub.list(platform, path)), unwrap),
  stat: (path: string) => Effect.flatMap(callWorkspace((stub) => stub.stat(platform, path)), unwrap),
  audit: (after: number) => callWorkspace((stub) => stub.audit(after)),
};

// ---------------------------------------------------------------------------
// Tools for LLM agents: `toLayer(impl, { toolkit: Workspace.Toolkit })` + `Layer.provide(Workspace.ToolkitLayer)`
// ---------------------------------------------------------------------------

const Failure = Schema.Union([FsDenied, FileNotFound]);

const ReadFile = Tool.make("read_file", {
  description: "Read a text file from the workspace. Paths: ~/… is your home, shared/… is the shared area.",
  parameters: Schema.Struct({ path: Schema.String }),
  success: Schema.String,
  failure: Failure,
  // The model sees a denial as a typed result (`_tag: "FsDenied"`) and can work around it.
  failureMode: "return",
  dependencies: [AgentContext],
});
const WriteFile = Tool.make("write_file", {
  description: "Write (replace) a text file in the workspace. You can always write under ~/.",
  parameters: Schema.Struct({ path: Schema.String, content: Schema.String }),
  success: FileRef,
  failure: Failure,
  // The model sees a denial as a typed result (`_tag: "FsDenied"`) and can work around it.
  failureMode: "return",
  dependencies: [AgentContext],
});
const ListFiles = Tool.make("list_files", {
  description: "List the files and directories (ending in /) directly under a workspace directory.",
  parameters: Schema.Struct({ path: Schema.String }),
  success: Schema.Array(Schema.String),
  failure: Failure,
  // The model sees a denial as a typed result (`_tag: "FsDenied"`) and can work around it.
  failureMode: "return",
  dependencies: [AgentContext],
});
const ShareFile = Tool.make("share_file", {
  description: "Give another agent read or write access to a file or directory in your home (~/…).",
  parameters: Schema.Struct({ path: Schema.String, agentType: Schema.String, agentName: Schema.String, access: AccessSchema }),
  success: Schema.Struct({ shared: Schema.String }),
  failure: Failure,
  // The model sees a denial as a typed result (`_tag: "FsDenied"`) and can work around it.
  failureMode: "return",
  dependencies: [AgentContext],
});

/** The workspace tools: read_file, write_file, list_files, share_file. */
export const Toolkit = Toolkit_.make(ReadFile, WriteFile, ListFiles, ShareFile);

// An unreachable workspace becomes a failed tool result the model sees (the session catches defects).
export const ToolkitLayer = Toolkit.toLayer(
  Toolkit.of({
    read_file: ({ path }) => read(path).pipe(Effect.catchTag("AgentError", Effect.die)),
    write_file: ({ path, content }) => write(path, content).pipe(Effect.catchTag("AgentError", Effect.die)),
    list_files: ({ path }) => list(path).pipe(Effect.catchTag("AgentError", Effect.die)),
    share_file: ({ path, agentType, agentName, access }) =>
      share(path, { type: agentType, name: agentName }, access).pipe(
        Effect.as({ shared: `${path} with ${agentType}/${agentName} (${access})` }),
        Effect.catchTag("AgentError", Effect.die),
      ),
  }),
);
