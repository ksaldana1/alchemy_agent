// M1 deployed check. Run from the repo root:
//   bun run hello:check <url> all                  happy path, retry, duplicate key
//   bun run hello:check <url> redeploy-start       sends a task that will be pending ~30 s
//   (redeploy now: bun run hello:deploy --stage dev_$USER --yes --force)
//   bun run hello:check <url> redeploy-finish <ref> waits for that task to complete
// The token comes from AGENT_API_TOKEN (examples/hello/.env is loaded automatically).
import { Api } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { Greeter } from "../src/greeter.ts";

const [url, step, ref] = process.argv.slice(2);
const token = process.env[Api.TOKEN_ENV];
if (!url || !step || !token) {
  console.error("usage: check.ts <url> all|redeploy-start|redeploy-finish <ref>  (needs AGENT_API_TOKEN)");
  process.exit(1);
}
const run = Date.now().toString(36);
const ok = (label: string, value: unknown) => console.log(`✓ ${label}: ${JSON.stringify(value)}`);

const program = Effect.gen(function* () {
  const greeters = yield* Greeter.client;
  const greeter = greeters(`check-${run}`);
  if (step === "all") {
    ok("happy path", yield* greeter.Greet({ name: "Ada" }));
    ok("throws once, succeeds on attempt 2", yield* greeter.FailOnce({ key: "once" }));
    const first = yield* greeter.Welcome({ key: "dup", name: "first" });
    const again = yield* greeter.Welcome({ key: "dup", name: "second" });
    if (first.greeting !== again.greeting) return yield* Effect.die(new Error("duplicate key ran again"));
    ok("duplicate key is a no-op", { first, again });
  } else if (step === "redeploy-start") {
    // Sent without waiting; its first attempt fails and its retry waits 30 s.
    yield* greeter.FailOnceSlowly({ key: "slow" }, { discard: true });
    yield* Effect.sleep("3 seconds");
    ok("sent; pending its 30 s retry", { agent: `check-${run}` });
    console.log(`\nnow redeploy, then: bun run hello:check ${url} redeploy-finish check-${run}`);
  } else if (step === "redeploy-finish" && ref) {
    // The same key is the same task: this waits for the one already pending.
    ok("completed after the redeploy", yield* greeters(ref).FailOnceSlowly({ key: "slow" }));
  }
});

await Effect.runPromise(program.pipe(Effect.provide(Api.connect({ url, token })), Effect.provide(FetchHttpClient.layer)));
