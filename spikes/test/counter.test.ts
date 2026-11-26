import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Test from "@alchemy.run/test/Bun";
import { expect } from "bun:test";
import * as Effect from "effect/Effect";
import Stack from "../alchemy.run.ts";

// Cloud by default (stage test_$USER). ALCHEMY_TEST_DEV=1 runs the same suite in local workerd.
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});

const stack = beforeAll(deploy(Stack), { timeout: 300_000 });
// Tear the stage down after the suite. NO_DESTROY=1 keeps it for debugging.
afterAll.skipIf(!!process.env.NO_DESTROY)(destroy(Stack));

test(
  "counter increments and reads back",
  Effect.gen(function* () {
    const { url } = yield* stack;
    // Fresh name per run: the deployed DO keeps its count across test runs.
    const base = `${url}/counter/t-${crypto.randomUUID()}`;
    const count = (path: string) =>
      Test.getWhenReady(`${base}/${path}`).pipe(
        Effect.flatMap((res) => res.json),
        Effect.map((body) => (body as { count: number }).count),
      );

    expect(yield* count("increment")).toBe(1);
    expect(yield* count("increment")).toBe(2);
    expect(yield* count("get")).toBe(2);
  }),
  { timeout: 60_000 },
);
