// Listeners (M6): file events and cron fires create work exactly once, through one queue with
// retries and a dead-letter queue. Local runs put events on the queue by hand (local R2 sends no
// notifications); the cloud run also checks a real upload.
//
// Local by default (`bun run test`); `bun run test:cloud` runs the same file on test_$USER.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Schedule from "effect/Schedule";
import * as Api from "../src/Api.ts";
import Stack from "./fixtures/stack.ts";

process.env[Api.TOKEN_ENV] ??= "test-token";
const token = process.env[Api.TOKEN_ENV]!;

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

const run = crypto.randomUUID().slice(0, 8);
const timeout = { timeout: 180_000 };
const local = !!process.env.ALCHEMY_TEST_DEV;

type LogEntry = { seq: number; kind: string; listener: string | null; eventId: string | null; detail: string; attempts: number; message: any };

const http = (request: HttpClientRequest.HttpClientRequest) =>
  Effect.flatMap(stack, ({ url }) =>
    Test.executeWhenReady(request.pipe(HttpClientRequest.prependUrl(url!), HttpClientRequest.bearerToken(token))).pipe(
      Effect.flatMap((res) => Effect.map(res.text, (text) => (text === "" ? null : (JSON.parse(text) as any)))),
    ),
  );

const put = (path: string, text: string) =>
  http(HttpClientRequest.put(`/workspace/files/${path}`).pipe(HttpClientRequest.setBody(HttpBody.text(text)))) as Effect.Effect<{
    path: string;
    revision: string;
    size: number;
  }>;
/** Puts a message on the listeners' queue, as a replay would. */
const inject = (message: unknown) => http(HttpClientRequest.post(`/listeners/events`).pipe(HttpClientRequest.setBody(HttpBody.jsonUnsafe({ message }))));
/** The R2 notification for a written file. */
const r2Event = (ref: { path: string; revision: string; size: number }) => ({
  account: "test",
  action: "PutObject",
  bucket: "test",
  object: { key: ref.path, size: ref.size, eTag: ref.revision },
  eventTime: new Date().toISOString(),
});
const log = http(HttpClientRequest.get(`/listeners/log`)).pipe(Effect.map((entries) => entries as ReadonlyArray<LogEntry>));
/** Log entries for an event, once there are at least `n` of `kind`. */
const entriesFor = (pred: (e: LogEntry) => boolean, kind: string, n: number) =>
  log.pipe(
    Effect.map((all) => all.filter(pred)),
    Effect.repeat({ schedule: Schedule.spaced("500 millis"), until: (es) => es.filter((e) => e.kind === kind).length >= n, times: 240 }),
  );
const task = (type: string, name: string, taskId: string) =>
  http(HttpClientRequest.get(`/agents/${type}/${name}/tasks/${encodeURIComponent(taskId)}`));
const status = (workflow: string, id: string) => http(HttpClientRequest.get(`/workflows/${workflow}/runs/${id}`));

test(
  "a file event starts one run; the same event again starts nothing new",
  Effect.gen(function* () {
    const ref = yield* put(`shared/inbox/${run}-a.txt`, "hello");
    const eventId = `${ref.path}@${ref.revision}`;
    if (local) yield* inject(r2Event(ref)); // local R2 sends no notifications
    yield* entriesFor((e) => e.eventId === eventId, "delivered", 1);
    yield* inject(r2Event(ref)); // a replay by hand
    const delivered = (yield* entriesFor((e) => e.eventId === eventId, "delivered", 2)).filter((e) => e.kind === "delivered");
    // Both deliveries name the same run: the run's key is the file's name.
    expect(new Set(delivered.map((e) => e.detail)).size).toBe(1);
    const executionId = delivered[0]!.detail.split("/").at(-1)!;
    const done = yield* status("Greeting", executionId).pipe(
      Effect.repeat({ schedule: Schedule.spaced("500 millis"), until: (s) => s._tag !== "Running", times: 120 }),
    );
    expect(done).toMatchObject({ _tag: "Completed" });
    expect(yield* task("Echo", `wf-a-${run}-a`, `wf/Greeting/${executionId}/c0`)).toMatchObject({ status: "completed", attempts: 1 });
  }),
  timeout,
);

test(
  "a file event creates one agent task, keyed by the Rpc's primaryKey",
  Effect.gen(function* () {
    const ref = yield* put(`shared/tasks/${run}-b.txt`, "x");
    yield* inject(r2Event(ref));
    yield* inject(r2Event(ref));
    yield* entriesFor((e) => e.eventId === `${ref.path}@${ref.revision}`, "delivered", local ? 2 : 2);
    expect(yield* task("Echo", "listened", `EchoKeyed:${ref.path}`)).toMatchObject({
      status: "completed",
      attempts: 1,
      causation: `listener/files:shared/tasks/`,
      result: { echoed: `${run.toUpperCase()}-B` },
    });
  }),
  timeout,
);

test(
  "a cron fire creates one task per scheduled time",
  Effect.gen(function* () {
    const scheduledTime = Date.now();
    const fire = { kind: "cron", listener: "tick", cron: "0 0 1 1 *", scheduledTime };
    yield* inject(fire);
    yield* inject(fire); // Cloudflare may retry a fire; a replay is the same fire
    yield* entriesFor((e) => e.listener === "tick" && e.eventId === String(scheduledTime), "delivered", 2);
    expect(yield* task("Echo", "ticker", `listener/tick/${scheduledTime}`)).toMatchObject({
      status: "completed",
      attempts: 1,
      result: { echoed: `TICK ${scheduledTime}` },
    });
  }),
  timeout,
);

test(
  "a poison event is retried, then dead-lettered and recorded",
  Effect.gen(function* () {
    const path = `shared/poison/${run}.txt`;
    yield* inject({ action: "PutObject", object: { key: path, size: 1, eTag: "e" } });
    const entries = yield* entriesFor((e) => e.message?.object?.key === path, "dead-letter", 1);
    expect(entries.filter((e) => e.kind === "failed").length).toBeGreaterThanOrEqual(2);
    expect(entries.find((e) => e.kind === "failed")?.detail).toMatch(/poison/);
  }),
  timeout,
);

test.skipIf(local)(
  "a real upload starts a run without any replay",
  Effect.gen(function* () {
    const ref = yield* put(`shared/inbox/${run}-real.txt`, "uploaded");
    const delivered = yield* entriesFor((e) => e.eventId === `${ref.path}@${ref.revision}`, "delivered", 1);
    expect(delivered[0]!.detail).toMatch(/^run Greeting\//);
  }),
  timeout,
);
