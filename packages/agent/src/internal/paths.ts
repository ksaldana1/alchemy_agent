// Workspace paths and the permission decision. Pure functions: the Workspace Durable Object
// (`internal/workspace.ts`) feeds them its grants and records what they decide.
//
//   agents/<type>/<name>/…   an agent's home ("~/…" from its own point of view); the owner has full access
//   shared/…                 the shared area; only the platform grants access to it
import type { Address } from "./core.ts";

export type Access = "read" | "write";

/** Who is asking: an agent (determined by framework code), or the platform itself. */
export type Caller = Address | null;

export interface GrantRow {
  readonly grantee: string;
  readonly path: string;
  readonly access: Access;
}

/** A path the workspace refuses before any grant is looked at. */
export class PathRejected {
  readonly _tag = "PathRejected";
  constructor(readonly reason: string) {}
}

/** `Type/name`: how grants and the audit name an agent. */
export const agentKey = (agent: Address) => `${agent.type}/${agent.name}`;

/** An agent's home, as a logical path. */
export const homeOf = (agent: Address) => `agents/${agent.type}/${agent.name}`;

/**
 * `~/x` → `agents/<type>/<name>/x`; resolves `.` and `..` (before any permission check); drops
 * trailing slashes. Rejects absolute paths, paths that escape the root, the root itself, and
 * anything outside `agents/` and `shared/`.
 */
export const normalize = (caller: Caller, raw: string): string | PathRejected => {
  if (raw.includes("\0") || raw.includes("\\")) return new PathRejected(`invalid path: ${JSON.stringify(raw)}`);
  let expanded = raw;
  if (raw === "~" || raw.startsWith("~/")) {
    if (caller === null) return new PathRejected(`"~" needs an agent: ${raw}`);
    expanded = homeOf(caller) + raw.slice(1);
  }
  if (expanded.startsWith("/")) return new PathRejected(`absolute paths are not allowed: ${raw}`);
  const parts: Array<string> = [];
  for (const part of expanded.split("/")) {
    if (part === "" || part === ".") continue;
    if (part === "..") {
      if (parts.length === 0) return new PathRejected(`the path escapes the workspace: ${raw}`);
      parts.pop();
    } else parts.push(part);
  }
  if (parts.length === 0) return new PathRejected("the workspace root itself is not accessible");
  if (parts[0] !== "agents" && parts[0] !== "shared") return new PathRejected(`not in the workspace (agents/ or shared/): ${raw}`);
  return parts.join("/");
};

/** The agent whose home holds `path` (`Type/name`), or null for the shared area. */
export const ownerOf = (path: string): string | null => {
  const m = /^agents\/([^/]+)\/([^/]+)(\/|$)/.exec(path);
  return m ? `${m[1]}/${m[2]}` : null;
};

/** Does a grant on `prefix` cover `path`? The prefix itself or anything below it, never a sibling. */
export const covers = (prefix: string, path: string) => path === prefix || path.startsWith(`${prefix}/`);

export interface Decision {
  readonly allowed: boolean;
  readonly reason: string;
}

/**
 * May `caller` have `need` access to (normalized) `path`? The platform may do anything, an
 * owner anything in its home; otherwise the most specific grant covering the path decides.
 */
export const decide = (caller: Caller, path: string, need: Access, grants: ReadonlyArray<GrantRow>): Decision => {
  if (caller === null) return { allowed: true, reason: "platform" };
  const key = agentKey(caller);
  if (ownerOf(path) === key) return { allowed: true, reason: "owner" };
  let best: GrantRow | undefined;
  for (const g of grants) {
    if (g.grantee === key && covers(g.path, path) && (best === undefined || g.path.length > best.path.length)) best = g;
  }
  if (best !== undefined && (need === "read" || best.access === "write")) {
    return { allowed: true, reason: `grant ${best.access} on ${best.path}` };
  }
  return { allowed: false, reason: best ? `the grant on ${best.path} is read-only` : "no grant" };
};

/** Only an owner shares or revokes, and only inside its own home. The platform grants by declaration. */
export const decideShare = (caller: Caller, path: string): Decision => {
  if (caller !== null && ownerOf(path) === agentKey(caller)) return { allowed: true, reason: "owner" };
  return { allowed: false, reason: "only the owner can share" };
};
