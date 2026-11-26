// M1 deployed check. Run from the repo root:
//   bun run hello:check <url> all                  happy path, retry, duplicate id
//   bun run hello:check <url> redeploy-start       sends a task that will be pending ~30 s
//   (redeploy now: bun run hello:deploy --stage dev_$USER --yes)
//   bun run hello:check <url> redeploy-finish <id> waits for that task to complete
// The token comes from AGENT_API_TOKEN (examples/hello/.env is loaded automatically).
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { Api } from "alchemy-agent";
import { Greeter } from "../src/greeter.ts";

const [url, step, taskId] = process.argv.slice(2);
const token = process.env[Api.TOKEN_ENV];
if (!url || !step || !token) {
  console.error("usage: check.ts <url> all|redeploy-start|redeploy-finish <taskId>  (needs AGENT_API_TOKEN)");
  process.exit(1);
}
const run = Date.now().toString(36);
const greeter = Api.client({ url, token }).agent(Greeter, `check-${run}`);
const ok = (label: string, value: unknown) => console.log(`✓ ${label}: ${JSON.stringify(value)}`);

const program = Effect.gen(function* () {
  if (step === "all") {
    const hello = yield* greeter.send("greet", { name: "Ada" });
    ok("happy path", yield* hello.result());

    const flaky = yield* greeter.send("failOnce", {});
    ok("throws once, succeeds on attempt 2", yield* hello.result().pipe(Effect.andThen(flaky.result())));
    const flakyStatus = yield* flaky.status;
    ok("  record", { attempts: flakyStatus.attempts, error: flakyStatus.error });

    const first = yield* greeter.send("greet", { name: "first" }, { taskId: "dup" });
    const firstOut = yield* first.result();
    const again = yield* greeter.send("greet", { name: "second" }, { taskId: "dup" });
    const againOut = yield* again.result();
    if (JSON.stringify(firstOut) !== JSON.stringify(againOut)) {
      return yield* Effect.die(new Error("duplicate task id ran again"));
    }
    ok("duplicate task id is a no-op", { first: firstOut, again: againOut, attempts: (yield* again.status).attempts });
  } else if (step === "redeploy-start") {
    const handle = yield* greeter.send("failOnceSlowly", {});
    // Wait until the first attempt has failed and the task is pending its retry.
    const status = yield* handle.status.pipe(
      Effect.repeat({ until: (s) => s.attempts === 1 && s.status === "pending", times: 50 }),
    );
    ok("pending across the redeploy", { agent: greeter.name, taskId: handle.taskId, status: status.status, attempts: status.attempts });
    console.log(`\nnow redeploy, then: bun run hello:check ${url} redeploy-finish ${greeter.name}/${handle.taskId}`);
  } else if (step === "redeploy-finish" && taskId) {
    const [name, id] = taskId.split("/") as [string, string];
    const handle = Api.client({ url, token }).agent(Greeter, name).handle("failOnceSlowly", id);
    ok("completed after the redeploy", yield* handle.result());
    const s = yield* handle.status;
    ok("  record", { status: s.status, attempts: s.attempts, error: s.error });
  }
});

await Effect.runPromise(program.pipe(Effect.provide(FetchHttpClient.layer)));
