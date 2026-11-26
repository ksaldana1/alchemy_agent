// Workspace paths and permission decisions, ported from mini-agent-runtime/tests/filesystem.test.ts
// ("AgentFilesystem (platform-checked permissions)"). Pure: no Worker needed. The same rules run
// end to end in workspace.test.ts.
import { describe, expect, test } from "bun:test";
import { decide, decideShare, normalize, ownerOf, PathRejected, type GrantRow } from "../src/internal/paths.ts";

const alice = { type: "Probe", name: "alice" };
const bob = { type: "Probe", name: "bob" };
const aliceFile = (name: string) => `agents/Probe/alice/${name}`;

const path = (caller: typeof alice | null, raw: string) => {
  const p = normalize(caller, raw);
  if (p instanceof PathRejected) throw new Error(p.reason);
  return p;
};
const rejected = (caller: typeof alice | null, raw: string) => {
  const p = normalize(caller, raw);
  return p instanceof PathRejected ? p.reason : undefined;
};
const grant = (grantee: typeof alice, p: string, access: "read" | "write"): GrantRow => ({ grantee: `${grantee.type}/${grantee.name}`, path: p, access });

describe("paths", () => {
  test("~ is the caller's home", () => {
    expect(path(alice, "~")).toBe("agents/Probe/alice");
    expect(path(alice, "~/notes/a.txt")).toBe(aliceFile("notes/a.txt"));
    expect(path(alice, "shared/documents/")).toBe("shared/documents");
    expect(rejected(null, "~/x")).toMatch(/needs an agent/);
  });

  test("rejects absolute paths, escapes, the root, and anything outside agents/ and shared/", () => {
    expect(rejected(alice, "/etc/passwd")).toMatch(/absolute/);
    expect(rejected(alice, "../outside")).toMatch(/escapes/);
    expect(rejected(alice, "~/../../../../outside")).toMatch(/escapes/);
    expect(rejected(alice, ".")).toMatch(/root/);
    expect(rejected(alice, "tmp/x")).toMatch(/not in the workspace/);
    expect(rejected(alice, "shared/a\0b")).toMatch(/invalid/);
  });

  test("'..' is resolved before the permission check", () => {
    const p = path(bob, "~/../alice/a.txt");
    expect(p).toBe(aliceFile("a.txt"));
    expect(decide(bob, p, "read", [])).toEqual({ allowed: false, reason: "no grant" });
  });

  test("owner of a path", () => {
    expect(ownerOf(aliceFile("x"))).toBe("Probe/alice");
    expect(ownerOf("agents/Probe/alice")).toBe("Probe/alice");
    expect(ownerOf("agents/Probe")).toBeNull();
    expect(ownerOf("shared/documents/x")).toBeNull();
  });
});

describe("decisions", () => {
  test("owners have read/write on their home; others have nothing", () => {
    expect(decide(alice, aliceFile("a.txt"), "write", []).allowed).toBe(true);
    expect(decide(bob, aliceFile("a.txt"), "read", [])).toEqual({ allowed: false, reason: "no grant" });
    expect(decide(bob, aliceFile("b.txt"), "write", []).allowed).toBe(false);
  });

  test("the shared area is closed until the platform grants access", () => {
    expect(decide(alice, "shared/documents/d.txt", "read", []).allowed).toBe(false);
    const grants = [grant(alice, "shared/documents", "read")];
    expect(decide(alice, "shared/documents/d.txt", "read", grants).allowed).toBe(true);
    expect(decide(alice, "shared/documents/e.txt", "write", grants).reason).toMatch(/read-only/);
    expect(decide(alice, "shared/artifacts/x", "read", grants).allowed).toBe(false);
  });

  test("a directory grant covers its subtree but not a sibling with the same prefix", () => {
    const grants = [grant(alice, "shared/documents", "write")];
    expect(decide(alice, "shared/documents/deep/nested.txt", "write", grants).allowed).toBe(true);
    expect(decide(alice, "shared/documents2/x.txt", "write", grants).allowed).toBe(false);
  });

  test("the most specific grant wins", () => {
    const grants = [grant(alice, "shared/artifacts", "write"), grant(alice, "shared/artifacts/locked", "read")];
    expect(decide(alice, "shared/artifacts/free.txt", "write", grants).allowed).toBe(true);
    expect(decide(alice, "shared/artifacts/locked/x.txt", "write", grants).reason).toMatch(/read-only/);
  });

  test("a grant to one agent says nothing about another", () => {
    expect(decide(bob, "shared/documents/d.txt", "read", [grant(alice, "shared/documents", "read")]).allowed).toBe(false);
  });

  test("an agent can share one file without exposing the rest of its home", () => {
    const grants = [grant(bob, aliceFile("report.txt"), "read")];
    expect(decide(bob, aliceFile("report.txt"), "read", grants).allowed).toBe(true);
    expect(decide(bob, aliceFile("secret.txt"), "read", grants).allowed).toBe(false);
    expect(decide(bob, "agents/Probe/alice", "read", grants).allowed).toBe(false);
    expect(decide(bob, aliceFile("report.txt"), "write", grants).reason).toMatch(/read-only/);
  });

  test("only the owner can share, and never the shared area", () => {
    expect(decideShare(alice, aliceFile("r.txt")).allowed).toBe(true);
    expect(decideShare(bob, aliceFile("r.txt"))).toEqual({ allowed: false, reason: "only the owner can share" });
    expect(decideShare(bob, "shared/documents").allowed).toBe(false);
    expect(decideShare(null, "shared/documents").allowed).toBe(false);
  });

  test("the platform may do anything", () => {
    expect(decide(null, "shared/documents/x", "write", []).allowed).toBe(true);
  });
});
