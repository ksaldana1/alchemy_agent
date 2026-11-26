import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";

// Spike 0.1: the smallest SQLite-backed Durable Object. One count per instance name.
export default class Counter extends Cloudflare.DurableObject<Counter>()(
  "Counter",
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    return Effect.gen(function* () {
      let count = (yield* state.storage.get<number>("count")) ?? 0;
      return {
        increment: () =>
          Effect.gen(function* () {
            count += 1;
            yield* state.storage.put("count", count);
            return count;
          }),
        get: () => Effect.succeed(count),
      };
    });
  }),
) {}
