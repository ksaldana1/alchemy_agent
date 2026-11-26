// The Workspace Durable Object (M4, D8): one instance, `workspace`, holds every grant and the
// audit in its SQLite, checks each operation, records the decision, and only then touches R2.
// A check and its audit row commit together. File contents live in R2 under their logical path.
import * as Cloudflare from "@alchemy.run/cloudflare";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import * as Effect from "effect/Effect";
import type { Address } from "./core.ts";
import type { Sql } from "./inbox.ts";
import { agentKey, decide, decideShare, normalize, PathRejected, type Access, type Caller, type GrantRow } from "./paths.ts";

export const WORKSPACE_TYPE = "AgentWorkspace";
export const WORKSPACE_NAME = "workspace";
export const BUCKET_ID = "AgentWorkspaceFiles";
const SCHEMA_VERSION = 1;

/** A file as of one revision. `revision` is R2's etag: it changes with every write. */
export interface FileRef {
  readonly path: string;
  readonly revision: string;
  readonly size: number;
}

/** Who asks, and on behalf of which task (for the audit). Filled in by framework code. */
export interface RequestContext {
  readonly caller: Caller;
  readonly taskId: string | null;
  readonly attempt: number | null;
}

export type WorkspaceFailure =
  | { readonly _tag: "FsDenied"; readonly op: string; readonly path: string; readonly message: string }
  | { readonly _tag: "FileNotFound"; readonly path: string };

/** Plain data across the RPC boundary; the client turns failures back into errors. */
export type Result<A> = { readonly ok: true; readonly value: A } | { readonly ok: false; readonly error: WorkspaceFailure };

export interface AuditEntry {
  readonly seq: number;
  readonly at: number;
  /** `Type/name`, or `platform`. */
  readonly caller: string;
  readonly op: string;
  readonly path: string;
  readonly allowed: boolean;
  readonly reason: string;
  readonly taskId: string | null;
  readonly attempt: number | null;
}

export interface Grant {
  readonly grantee: Address;
  readonly path: string;
  readonly access: Access;
}

/**
 * The Workspace Durable Object's RPC surface. Framework-internal. Only add methods: after a
 * deploy an instance can run the previous code for a few minutes.
 */
export type WorkspaceRpc = {
  read: (ctx: RequestContext, path: string) => Effect.Effect<Result<{ readonly text: string; readonly ref: FileRef }>, never, any>;
  write: (ctx: RequestContext, path: string, text: string) => Effect.Effect<Result<FileRef>, never, any>;
  list: (ctx: RequestContext, path: string) => Effect.Effect<Result<ReadonlyArray<string>>, never, any>;
  stat: (ctx: RequestContext, path: string) => Effect.Effect<Result<FileRef>, never, any>;
  copy: (ctx: RequestContext, from: string, to: string) => Effect.Effect<Result<FileRef>, never, any>;
  share: (ctx: RequestContext, path: string, grantee: Address, access: Access) => Effect.Effect<Result<void>, never, any>;
  revoke: (ctx: RequestContext, path: string, grantee: Address) => Effect.Effect<Result<void>, never, any>;
  audit: (after: number) => Effect.Effect<ReadonlyArray<AuditEntry>, never, any>;
};

export interface WorkspaceConfig {
  /** Platform grants, seeded when the object activates (the only way into `shared/`). */
  readonly grants: ReadonlyArray<Grant>;
  /** Empty the bucket when the stack is destroyed. Default false (destroy fails on a non-empty bucket). */
  readonly forceDestroy?: boolean | undefined;
}

const ok = <A>(value: A): Result<A> => ({ ok: true, value });
const denied = (op: string, path: string, reason: string): Result<never> => ({
  ok: false,
  error: { _tag: "FsDenied", op, path, message: `${op} ${path}: ${reason}` },
});
const notFound = (path: string): Result<never> => ({ ok: false, error: { _tag: "FileNotFound", path } });

const migrate = (sql: Sql) =>
  Effect.gen(function* () {
    yield* sql.exec("CREATE TABLE IF NOT EXISTS alchemy_workspace_schema (version INTEGER NOT NULL)");
    const rows = yield* Effect.flatMap(sql.exec<{ version: number }>("SELECT version FROM alchemy_workspace_schema"), (c) => c.toArray());
    const version = rows[0]?.version ?? 0;
    if (version >= SCHEMA_VERSION) return;
    yield* sql.exec(`CREATE TABLE IF NOT EXISTS alchemy_workspace_grants (
      grantee TEXT NOT NULL,
      path TEXT NOT NULL,
      access TEXT NOT NULL,
      granted_by TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      PRIMARY KEY (grantee, path)
    )`);
    yield* sql.exec(`CREATE TABLE IF NOT EXISTS alchemy_workspace_audit (
      seq INTEGER PRIMARY KEY AUTOINCREMENT,
      at INTEGER NOT NULL,
      caller TEXT NOT NULL,
      op TEXT NOT NULL,
      path TEXT NOT NULL,
      allowed INTEGER NOT NULL,
      reason TEXT NOT NULL,
      task_id TEXT,
      attempt INTEGER
    )`);
    yield* sql.exec("DELETE FROM alchemy_workspace_schema");
    yield* sql.exec("INSERT INTO alchemy_workspace_schema (version) VALUES (?)", SCHEMA_VERSION);
  });

/** The construction Effect for the Workspace Durable Object class. */
export const workspaceObject = (config: WorkspaceConfig) =>
  Effect.gen(function* () {
    const files = yield* Cloudflare.R2.ReadWriteBucket(
      Cloudflare.R2.Bucket(BUCKET_ID, config.forceDestroy ? { forceDestroy: true } : {}),
    );
    const state = yield* Cloudflare.DurableObjectState;

    return Effect.gen(function* () {
      const sql = state.storage.sql as unknown as Sql;
      const transaction = <A>(effect: Effect.Effect<A, never, RuntimeContext>) =>
        state.storage.transaction(effect) as Effect.Effect<A, never, RuntimeContext>;
      const all = <T extends Record<string, any>>(query: string, ...bindings: Array<unknown>) =>
        Effect.flatMap(sql.exec<T>(query, ...bindings), (c) => c.toArray());
      yield* transaction(migrate(sql));

      // Declared platform grants replace the previous declaration. Agents' shares are untouched.
      yield* transaction(
        Effect.gen(function* () {
          yield* sql.exec("DELETE FROM alchemy_workspace_grants WHERE granted_by = 'platform'");
          for (const g of config.grants) {
            const path = normalize(null, g.path);
            if (path instanceof PathRejected) {
              yield* Effect.logWarning(`workspace: ignoring the declared grant on ${g.path}: ${path.reason}`);
              continue;
            }
            yield* sql.exec(
              `INSERT INTO alchemy_workspace_grants (grantee, path, access, granted_by, created_at) VALUES (?, ?, ?, 'platform', ?)
               ON CONFLICT (grantee, path) DO UPDATE SET access = excluded.access, granted_by = 'platform'`,
              agentKey(g.grantee),
              path,
              g.access,
              Date.now(),
            );
          }
        }),
      );

      const audit = (ctx: RequestContext, op: string, path: string, allowed: boolean, reason: string) =>
        Effect.andThen(
          sql.exec(
            "INSERT INTO alchemy_workspace_audit (at, caller, op, path, allowed, reason, task_id, attempt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            Date.now(),
            ctx.caller === null ? "platform" : agentKey(ctx.caller),
            op,
            path,
            allowed ? 1 : 0,
            reason,
            ctx.taskId,
            ctx.attempt,
          ),
          allowed ? Effect.void : Effect.logInfo("workspace.denied").pipe(
            Effect.annotateLogs({ caller: ctx.caller === null ? "platform" : agentKey(ctx.caller), op, path, reason, taskId: ctx.taskId }),
          ),
        );

      /** Normalizes and checks `raw` for `need`; the decision and its audit row commit together. */
      const authorize = (ctx: RequestContext, op: string, raw: string, need: Access) =>
        transaction(
          Effect.gen(function* () {
            const path = normalize(ctx.caller, raw);
            if (path instanceof PathRejected) {
              yield* audit(ctx, op, raw, false, path.reason);
              return denied(op, raw, path.reason);
            }
            const grants =
              ctx.caller === null
                ? []
                : yield* all<GrantRow>("SELECT grantee, path, access FROM alchemy_workspace_grants WHERE grantee = ?", agentKey(ctx.caller));
            const decision = decide(ctx.caller, path, need, grants);
            yield* audit(ctx, op, path, decision.allowed, decision.reason);
            return decision.allowed ? ok(path) : denied(op, path, decision.reason);
          }),
        );

      const authorizeShare = (ctx: RequestContext, op: string, raw: string) =>
        transaction(
          Effect.gen(function* () {
            const path = normalize(ctx.caller, raw);
            if (path instanceof PathRejected) {
              yield* audit(ctx, op, raw, false, path.reason);
              return denied(op, raw, path.reason);
            }
            const decision = decideShare(ctx.caller, path);
            yield* audit(ctx, op, path, decision.allowed, decision.reason);
            return decision.allowed ? ok(path) : denied(op, path, decision.reason);
          }),
        );

      const refOf = (path: string, object: { readonly etag: string; readonly size: number }): FileRef => ({
        path,
        revision: object.etag,
        size: object.size,
      });

      // R2 failures are infrastructure failures: the calling task retries.
      const readFile = (path: string) =>
        Effect.gen(function* () {
          const object = yield* files.get(path);
          if (object === null) return notFound(path);
          return ok({ text: yield* object.text(), ref: refOf(path, object) });
        }).pipe(Effect.orDie);
      const writeFile = (path: string, text: string) =>
        files.put(path, text, { httpMetadata: { contentType: "text/plain; charset=utf-8" } }).pipe(
          Effect.map((object) => ok(refOf(path, object))),
          Effect.orDie,
        );

      const rpc: WorkspaceRpc = {
        read: (ctx, raw) =>
          Effect.flatMap(authorize(ctx, "read", raw, "read"), (r) => (r.ok ? readFile(r.value) : Effect.succeed(r))),
        write: (ctx, raw, text) =>
          Effect.flatMap(authorize(ctx, "write", raw, "write"), (r) => (r.ok ? writeFile(r.value, text) : Effect.succeed(r))),
        stat: (ctx, raw) =>
          Effect.flatMap(authorize(ctx, "stat", raw, "read"), (r) =>
            r.ok
              ? files.head(r.value).pipe(
                  Effect.map((object) => (object === null ? notFound(r.value) : ok(refOf(r.value, object)))),
                  Effect.orDie,
                )
              : Effect.succeed(r),
          ),
        list: (ctx, raw) =>
          Effect.flatMap(authorize(ctx, "list", raw, "read"), (r) => {
            if (!r.ok) return Effect.succeed(r);
            const prefix = `${r.value}/`;
            return Effect.gen(function* () {
              const names: Array<string> = [];
              let cursor: string | undefined;
              do {
                const page = yield* files.list({ prefix, delimiter: "/", ...(cursor ? { cursor } : {}) });
                for (const o of page.objects) names.push(o.key.slice(prefix.length));
                for (const p of page.delimitedPrefixes) names.push(p.slice(prefix.length));
                cursor = page.truncated ? page.cursor : undefined;
              } while (cursor !== undefined);
              return ok(names.sort());
            }).pipe(Effect.orDie);
          }),
        copy: (ctx, rawFrom, rawTo) =>
          Effect.gen(function* () {
            const from = yield* authorize(ctx, "read", rawFrom, "read");
            if (!from.ok) return from;
            const to = yield* authorize(ctx, "write", rawTo, "write");
            if (!to.ok) return to;
            const file = yield* readFile(from.value);
            if (!file.ok) return file;
            return yield* writeFile(to.value, file.value.text);
          }),
        share: (ctx, raw, grantee, access) =>
          Effect.flatMap(authorizeShare(ctx, "share", raw), (r) =>
            r.ok
              ? sql
                  .exec(
                    `INSERT INTO alchemy_workspace_grants (grantee, path, access, granted_by, created_at) VALUES (?, ?, ?, ?, ?)
                     ON CONFLICT (grantee, path) DO UPDATE SET access = excluded.access, granted_by = excluded.granted_by`,
                    agentKey(grantee),
                    r.value,
                    access,
                    agentKey(ctx.caller!),
                    Date.now(),
                  )
                  .pipe(Effect.as(ok(undefined)))
              : Effect.succeed(r),
          ),
        revoke: (ctx, raw, grantee) =>
          Effect.flatMap(authorizeShare(ctx, "revoke", raw), (r) =>
            r.ok
              ? sql
                  .exec("DELETE FROM alchemy_workspace_grants WHERE grantee = ? AND path = ?", agentKey(grantee), r.value)
                  .pipe(Effect.as(ok(undefined)))
              : Effect.succeed(r),
          ),
        audit: (after) =>
          all<{
            seq: number;
            at: number;
            caller: string;
            op: string;
            path: string;
            allowed: number;
            reason: string;
            task_id: string | null;
            attempt: number | null;
          }>("SELECT * FROM alchemy_workspace_audit WHERE seq > ? ORDER BY seq LIMIT 500", after).pipe(
            Effect.map((rows) =>
              rows.map((r) => ({
                seq: r.seq,
                at: r.at,
                caller: r.caller,
                op: r.op,
                path: r.path,
                allowed: r.allowed === 1,
                reason: r.reason,
                taskId: r.task_id,
                attempt: r.attempt,
              })),
            ),
          ),
      };
      return rpc;
    });
  });
