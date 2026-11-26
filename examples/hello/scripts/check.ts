// M1 deployed check. Run from the repo root:
//   bun run hello:check <url> all                  happy path, retry, duplicate id
//   bun run hello:check <url> redeploy-start       sends a task that will be pending ~30 s
//   (redeploy now: bun run hello:deploy --stage dev_$USER --yes --force)
//   bun run hello:check <url> redeploy-finish <ref> waits for that task to complete
// The token comes from AGENT_API_TOKEN (examples/hello/.env is loaded automatically).
import { Api } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Schedule from "effect/Schedule";
import { Greeter } from "../src/greeter.ts";

const [url, step, ref] = process.argv.slice(2);
const token = process.env[Api.TOKEN_ENV];
if (!url || !step || !token) {
  console.error("usage: check.ts <url> all|redeploy-start|redeploy-finish <ref>  (needs AGENT_API_TOKEN)");
  process.exit(1);
}
const greeter = Greeter.named(`check-${Date.now().toString(36)}`);
const ok = (label: string, value: unknown) => console.log(`✓ ${label}: ${JSON.stringify(value)}`);

const program = Effect.gen(function* () {
  if (step === "all") {
    ok("happy path", yield* greeter.greet({ name: "Ada" }));

    ok("throws once, succeeds on attempt 2", yield* greeter.failOnce(undefined, { taskId: "once" }));
    const s = yield* greeter.handle("failOnce", "once").status;
    ok("  record", { attempts: s.attempts, error: s.error });

    const first = yield* greeter.greet({ name: "first" }, { taskId: "dup" });
    const again = yield* greeter.greet({ name: "second" }, { taskId: "dup" });
    if (JSON.stringify(first) !== JSON.stringify(again)) return yield* Effect.die(new Error("duplicate task id ran again"));
    ok("duplicate task id is a no-op", { first, again, attempts: (yield* greeter.handle("greet", "dup").status).attempts });
  } else if (step === "redeploy-start") {
    const handle = yield* greeter.send.failOnceSlowly();
    // Wait until the first attempt has failed and the task is pending its retry.
    const status = yield* handle.status.pipe(
      Effect.repeat({ schedule: Schedule.spaced("200 millis"), until: (r) => r.attempts === 1 && r.status === "pending" }),
    );
    ok("pending across the redeploy", { agent: greeter.address.name, taskId: handle.taskId, status: status.status, attempts: status.attempts });
    console.log(`\nnow redeploy, then: bun run hello:check ${url} redeploy-finish ${greeter.address.name}/${handle.taskId}`);
  } else if (step === "redeploy-finish" && ref) {
    const [name, taskId] = ref.split("/") as [string, string];
    const handle = Greeter.named(name).handle("failOnceSlowly", taskId);
    ok("completed after the redeploy", yield* handle.result);
    const s = yield* handle.status;
    ok("  record", { status: s.status, attempts: s.attempts, error: s.error });
  }
});

await Effect.runPromise(
  program.pipe(Effect.provide(Api.connect({ url, token }, Greeter)), Effect.provide(FetchHttpClient.layer)),
);
