// The acceptance test (M8, docs/NORTH-STAR.md): three submissions go through the whole newsroom
// while things go wrong. The editor dies on its first edit, and every storage event arrives twice.
// Each logical step must take effect once. A person (this test) signs off each article as its request appears.
//
// Local by default (`bun run newsroom:test`); `newsroom:test:cloud` runs it on test_$USER, where
// the storage events are real.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { Api, Platform } from "alchemy-agent";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import { Editor, Reviewer } from "../src/contracts.ts";
import { PublishArticle } from "../src/publish.ts";
import Stack from "./stack.ts";

process.env[Api.TOKEN_ENV] ??= "test-token";
const token = process.env[Api.TOKEN_ENV]!;
const local = !!process.env.ALCHEMY_TEST_DEV;

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({ providers: Cloudflare.providers(), state: Cloudflare.state() });
const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

const run = crypto.randomUUID().slice(0, 6);
const ARTICLES: Record<string, string> = {
  "bike-lanes": "The city council voted 7 to 2 on tuesday to aprove a network of protected bike lanes downtown.",
  library: "The central library will reopen in March after a two-year renovation, the city aproves the final budget.",
  budget: "The school board aproves a budget of $48 million for next year, up 3 percent.",
};

test(
  "every step takes effect once under chaos",
  Effect.gen(function* () {
    const { url } = yield* stack;
    // A fresh workers.dev URL can 404 for a while.
    yield* HttpClient.execute(HttpClientRequest.get(`${url}/agents`).pipe(HttpClientRequest.bearerToken(token))).pipe(
      Effect.filterOrFail((res) => res.status === 200),
      Effect.retry({ schedule: Schedule.spaced("1 second"), times: 60 }),
    );

    yield* Effect.gen(function* () {
      const newsroom = yield* Platform.Client;
      // Play the person: approve each sign-off request as it appears.
      yield* newsroom.requests.stream.pipe(
        Stream.take(3),
        Stream.runForEach((request) => request.respond({ approved: true }, { by: "editor-in-chief" })),
        Effect.forkScoped,
      );
      for (const [name, text] of Object.entries(ARTICLES)) {
        const ref = yield* newsroom.files.write(`shared/submissions/${run}-${name}.txt`, text);
        const event = { action: "PutObject", object: { key: ref.path, size: ref.size, eTag: ref.revision }, eventTime: new Date().toISOString() };
        if (local) yield* newsroom.listeners.replay(event); // local R2 sends no notifications
        yield* newsroom.listeners.replay(event); // and every event is delivered (again)
      }

      const runs = yield* newsroom.runs(PublishArticle).pipe(Stream.take(3), Stream.runCollect, Effect.timeout("3 minutes"));
      const published = yield* Effect.forEach(runs, (r) => r.result, { concurrency: "unbounded" });
      expect(published.map((p) => p.path).sort()).toEqual(Object.keys(ARTICLES).map((n) => `shared/published/${run}-${n}.md`).sort());

      // Duplicate events started no extra runs: each file's deliveries all name one run.
      yield* Effect.sleep("3 seconds");
      const log = (yield* HttpClient.execute(HttpClientRequest.get(`${url}/listeners/log`).pipe(HttpClientRequest.bearerToken(token))).pipe(
        Effect.flatMap((res) => res.json),
      )) as ReadonlyArray<{ kind: string; eventId: string | null; detail: string }>;
      const ours = log.filter((e) => e.kind === "delivered" && e.eventId?.includes(`/${run}-`));
      expect(ours.length).toBeGreaterThanOrEqual(6);
      expect(new Set(ours.map((e) => e.detail)).size).toBe(3);

      // The editor died once, on one article, and that edit was delivered again.
      const edits = yield* Effect.forEach(runs, (r) => Effect.map(r.tasksOn(Editor.address("desk")), (ts) => ts.filter((t) => t.task === "Edit")));
      expect(edits.map((ts) => ts.length)).toEqual([1, 1, 1]);
      expect(edits.map((ts) => ts[0]!.attempts).sort()).toEqual([1, 1, 2]);

      // The junior reviewer was never given access (a declared FsDenied); the senior reviewed each one.
      for (const r of runs) {
        const [junior] = yield* r.tasksOn(Reviewer.address("junior"));
        const [senior] = yield* r.tasksOn(Reviewer.address("senior"));
        expect(junior).toMatchObject({ status: "failed", error: { kind: "declared", value: { _tag: "FsDenied" } } });
        expect(senior).toMatchObject({ status: "completed", attempts: 1, result: { approved: true, by: "senior" } });
      }

      // The edit is what was published, typo fixed by the editor's write_file.
      const text = yield* newsroom.files.read(`shared/published/${run}-bike-lanes.md`);
      expect(text).toContain("to approve a network");
    }).pipe(Effect.provide(Platform.connect({ url: url!, token })), Effect.scoped);
  }),
  { timeout: 300_000 },
);
