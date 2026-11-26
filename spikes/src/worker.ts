import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import Counter from "./counter.ts";

// Spike Worker. Each spike owns one route prefix; 0.1 owns /counter.
export default Cloudflare.Worker(
  "SpikesApi",
  { main: import.meta.url },
  Effect.gen(function* () {
    const counters = yield* Counter;
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const path = new URL(request.url, "http://local").pathname;
        const counter = path.match(/^\/counter\/([^/]+)\/(increment|get)$/);
        if (counter) {
          const name = decodeURIComponent(counter[1]!);
          const stub = counters.getByName(name);
          const count = counter[2] === "increment" ? yield* stub.increment() : yield* stub.get();
          return yield* HttpServerResponse.json({ name, count, v: 2 });
        }
        return HttpServerResponse.text("Not found", { status: 404 });
      }),
    };
  }),
);
