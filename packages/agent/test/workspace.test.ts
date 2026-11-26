// The workspace end to end (M4), ported from mini-agent-runtime/tests/filesystem.test.ts ("ctx.fs:
// filesystem over IPC") plus the M4 checks: identity comes from the running task, denials are
// permanent and audited, and an LLM agent works through the workspace tools.
//
// Local by default (`bun run test`); `bun run test:cloud` runs the same file on test_$USER.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Agent from "../src/Agent.ts";
import * as Api from "../src/Api.ts";
import type * as Workspace from "../src/Workspace.ts";
import { Filer, Librarian } from "./fixtures/workspace.ts";
import Stack from "./fixtures/stack.ts";

process.env[Api.TOKEN_ENV] ??= "test-token";
const token = process.env[Api.TOKEN_ENV]!;

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

// Fresh names per run: deployed storage outlives a run. Agents holding platform grants have fixed
// names (alice, lib), so their files live under per-run directories.
const run = crypto.randomUUID().slice(0, 8);
const timeout = { timeout: 60_000 };

const viaApi = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.flatMap(stack, ({ url }) => effect.pipe(Effect.provide(Api.connect({ url: url!, token }))));

/** The platform's side of the workspace, over the HTTP API. */
const platform = {
  put: (path: string, text: string) =>
    Effect.flatMap(stack, ({ url }) =>
      HttpClient.execute(
        HttpClientRequest.put(`${url}/workspace/files/${path}`).pipe(
          HttpClientRequest.bearerToken(token),
          HttpClientRequest.setBody(HttpBody.text(text)),
        ),
      ).pipe(Effect.flatMap((res) => res.json)),
    ),
  get: (path: string) =>
    Effect.flatMap(stack, ({ url }) =>
      HttpClient.execute(HttpClientRequest.get(`${url}/workspace/files/${path}`).pipe(HttpClientRequest.bearerToken(token))).pipe(
        Effect.flatMap((res) => Effect.map(res.text, (text) => ({ status: res.status, text }))),
      ),
    ),
};

/** All audit entries (the endpoint pages by 500). */
const auditAll = Effect.flatMap(stack, ({ url }) =>
  Effect.gen(function* () {
    const out: Array<Workspace.AuditEntry> = [];
    let after = 0;
    for (;;) {
      const page = (yield* HttpClient.execute(
        HttpClientRequest.get(`${url}/workspace/audit?after=${after}`).pipe(HttpClientRequest.bearerToken(token)),
      ).pipe(Effect.flatMap((res) => res.json))) as unknown as ReadonlyArray<Workspace.AuditEntry>;
      out.push(...page);
      if (page.length < 500) return out;
      after = page.at(-1)!.seq;
    }
  }),
);

const home = (name: string) => `agents/Filer/${name}`;

test(
  "write / read / list in the agent's own home",
  viaApi(
    Effect.gen(function* () {
      const { url } = yield* stack;
      yield* Test.getWhenReady(`${url}/shout/warmup`);
      const me = (yield* Filer.client)(`own-${run}`);
      const ref = yield* me.Write({ path: "~/notes/a.txt", content: "hello" });
      expect(ref).toMatchObject({ path: `${home(`own-${run}`)}/notes/a.txt`, size: 5 });
      expect(ref.revision).toBeString();
      expect(yield* me.Read({ path: "~/notes/a.txt" })).toBe("hello");
      expect(yield* me.List({ path: "~/notes" })).toEqual(["a.txt"]);
      expect(yield* me.List({ path: "~" })).toEqual(["notes/"]);
      expect(yield* me.Revision({ path: ref.path })).toBe(ref.revision);
      // A new write is a new revision.
      const next = yield* me.Write({ path: "~/notes/a.txt", content: "hello again" });
      expect(next.revision).not.toBe(ref.revision);
    }),
  ),
  timeout,
);

test(
  "the shared area is closed until the platform grants access; grants are per agent",
  viaApi(
    Effect.gen(function* () {
      yield* platform.put(`shared/documents/${run}/in.txt`, "doc");
      const filers = yield* Filer.client;
      expect(yield* filers("alice").Read({ path: `shared/documents/${run}/in.txt` })).toBe("doc");
      // The same request from another agent is evaluated as that agent.
      const denied = yield* Effect.flip(filers(`nobody-${run}`).TryRead({ path: `shared/documents/${run}/in.txt` }));
      expect(denied).toMatchObject({ _tag: "FsDenied", op: "read" });
      // Read-only: alice can't write there.
      const error = yield* Effect.flip(filers("alice").Write({ path: `shared/documents/${run}/x.txt`, content: "" }));
      expect(error).toBeInstanceOf(Agent.AgentError);
      expect(error.reason).toMatchObject({ _tag: "FsDenied" });
      expect((error.reason as { message: string }).message).toMatch(/read-only/);
    }),
  ),
  timeout,
);

test(
  "the most specific grant wins; a directory grant doesn't cover a sibling with the same prefix",
  viaApi(
    Effect.gen(function* () {
      const alice = (yield* Filer.client)("alice");
      yield* alice.Write({ path: `shared/artifacts/${run}/free.txt`, content: "ok" });
      const locked = yield* Effect.flip(alice.Write({ path: `shared/artifacts/locked/${run}.txt`, content: "" }));
      expect(locked.reason).toMatchObject({ _tag: "FsDenied" });
      const sibling = yield* Effect.flip(alice.Write({ path: `shared/artifacts2/${run}.txt`, content: "" }));
      expect(sibling.reason).toMatchObject({ _tag: "FsDenied" });
    }),
  ),
  timeout,
);

test(
  "an undeclared denial settles the task at once (no retries); a declared one reaches the caller as itself",
  viaApi(
    Effect.gen(function* () {
      const filers = yield* Filer.client;
      yield* filers(`priv-${run}`).Write({ path: "~/secret.txt", content: "S" });
      const error = yield* Effect.flip(filers(`snoop-${run}`).Read({ path: `${home(`priv-${run}`)}/secret.txt` }));
      expect(error.reason).toMatchObject({ _tag: "FsDenied", op: "read", path: `${home(`priv-${run}`)}/secret.txt` });
      const declared = yield* Effect.flip(filers(`snoop-${run}`).TryRead({ path: `${home(`priv-${run}`)}/secret.txt` }));
      expect(declared._tag).toBe("FsDenied");
      // One attempt only: the denial was audited once.
      const denials = (yield* auditAll).filter((e) => e.caller === `Filer/snoop-${run}` && !e.allowed);
      expect(denials.map((e) => e.op)).toEqual(["read", "read"]);
      expect(denials.every((e) => e.attempt === 1 && e.reason === "no grant")).toBe(true);
    }),
  ),
  timeout,
);

test(
  "share and revoke a file the agent owns, without exposing the rest of its home",
  viaApi(
    Effect.gen(function* () {
      const filers = yield* Filer.client;
      const alice = filers(`sharer-${run}`);
      const bob = filers(`friend-${run}`);
      const report = `${home(`sharer-${run}`)}/report.txt`;
      yield* alice.Write({ path: "~/report.txt", content: "numbers" });
      yield* alice.Write({ path: "~/secret.txt", content: "S" });
      expect((yield* Effect.flip(bob.TryRead({ path: report })))._tag).toBe("FsDenied");

      yield* alice.Share({ path: "~/report.txt", to: Filer.address(`friend-${run}`), access: "read" });
      expect(yield* bob.TryRead({ path: report })).toBe("numbers");
      expect((yield* Effect.flip(bob.TryRead({ path: `${home(`sharer-${run}`)}/secret.txt` })))._tag).toBe("FsDenied");
      expect((yield* Effect.flip(bob.List({ path: home(`sharer-${run}`) })))._tag).toBe("FsDenied");
      expect((yield* Effect.flip(bob.Write({ path: report, content: "x" }))).reason).toMatchObject({ _tag: "FsDenied" });

      // Only the owner shares or revokes: a share can't be passed on, and the shared area is the platform's.
      const passOn = yield* Effect.flip(bob.Share({ path: report, to: Filer.address(`third-${run}`), access: "read" }));
      expect(passOn.message).toMatch(/only the owner can share/);
      expect((yield* Effect.flip(bob.Share({ path: "shared/documents", to: Filer.address(`friend-${run}`), access: "write" })))._tag).toBe(
        "FsDenied",
      );
      expect((yield* Effect.flip(bob.Revoke({ path: report, from: Filer.address(`friend-${run}`) })))._tag).toBe("FsDenied");

      yield* alice.Revoke({ path: "~/report.txt", from: Filer.address(`friend-${run}`) });
      expect((yield* Effect.flip(bob.TryRead({ path: report })))._tag).toBe("FsDenied");
    }),
  ),
  timeout,
);

test(
  "rejects absolute paths and paths that escape the workspace; '..' is resolved before the check",
  viaApi(
    Effect.gen(function* () {
      const filers = yield* Filer.client;
      const me = filers(`paths-${run}`);
      expect((yield* Effect.flip(me.TryRead({ path: "/etc/passwd" }))).message).toMatch(/absolute/);
      expect((yield* Effect.flip(me.TryRead({ path: "~/../../../../outside" }))).message).toMatch(/escapes/);
      expect((yield* Effect.flip(me.List({ path: "." }))).message).toMatch(/root/);
      yield* filers(`victim-${run}`).Write({ path: "~/a.txt", content: "A" });
      expect((yield* Effect.flip(me.TryRead({ path: `~/../victim-${run}/a.txt` }))).message).toMatch(/no grant/);
    }),
  ),
  timeout,
);

test(
  "copy needs read on the source and write on the target",
  viaApi(
    Effect.gen(function* () {
      yield* platform.put(`shared/documents/${run}/src.txt`, "copied text");
      const alice = (yield* Filer.client)("alice");
      const ref = yield* alice.Copy({ from: `shared/documents/${run}/src.txt`, to: `~/copies/${run}.txt` });
      expect(ref).toMatchObject({ path: `${home("alice")}/copies/${run}.txt`, size: 11 });
      expect(yield* alice.Read({ path: ref.path })).toBe("copied text");
      expect((yield* Effect.flip(alice.Copy({ from: ref.path, to: `shared/documents/${run}/back.txt` }))).message).toMatch(/read-only/);
      expect((yield* platform.get(`shared/documents/${run}/back.txt`)).status).toBe(404);
    }),
  ),
  timeout,
);

test(
  "records every decision in the audit log, with the task that asked",
  viaApi(
    Effect.gen(function* () {
      const filers = yield* Filer.client;
      yield* filers(`aud-a-${run}`).Write({ path: "~/a.txt", content: "" });
      yield* Effect.flip(filers(`aud-b-${run}`).TryRead({ path: `${home(`aud-a-${run}`)}/a.txt` }));
      yield* Effect.flip(filers(`aud-b-${run}`).TryRead({ path: "/etc/passwd" }));
      const mine = (yield* auditAll).filter((e) => e.caller.startsWith("Filer/aud-") && e.caller.endsWith(run));
      expect(mine.map((e) => [e.caller, e.op, e.allowed])).toEqual([
        [`Filer/aud-a-${run}`, "write", true],
        [`Filer/aud-b-${run}`, "read", false],
        [`Filer/aud-b-${run}`, "read", false],
      ]);
      expect(mine.every((e) => typeof e.taskId === "string")).toBe(true);
    }),
  ),
  timeout,
);

test(
  "an LLM agent reads a shared doc, writes ~/summary.md, and shares it; a third agent is denied and audited",
  viaApi(
    Effect.gen(function* () {
      yield* platform.put(`shared/docs/${run}/report.txt`, "The quarterly numbers are up.");
      const answer = yield* (yield* Librarian.client)("lib").Summarize({ path: `shared/docs/${run}/report.txt`, shareWith: `reader-${run}` });
      expect(answer).toBe("summarized and shared");

      const summary = "agents/Librarian/lib/summary.md";
      const filers = yield* Filer.client;
      expect(yield* filers(`reader-${run}`).TryRead({ path: summary })).toMatch(/^Summary of shared\/docs\/.*: quarterly numbers$/);
      expect((yield* Effect.flip(filers(`eve-${run}`).TryRead({ path: summary })))._tag).toBe("FsDenied");
      const denial = (yield* auditAll).find((e) => e.caller === `Filer/eve-${run}`);
      expect(denial).toMatchObject({ op: "read", path: summary, allowed: false, reason: "no grant" });
    }),
  ),
  timeout,
);

test(
  "a denied tool call is a failed tool result the model sees, not a failed turn",
  viaApi(
    Effect.gen(function* () {
      yield* (yield* Filer.client)(`hidden-${run}`).Write({ path: "~/x.txt", content: "x" });
      expect(yield* (yield* Librarian.client)("lib").Peek({ path: `${home(`hidden-${run}`)}/x.txt` })).toBe("the read was denied");
    }),
  ),
  timeout,
);
