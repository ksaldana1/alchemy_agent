// The test Worker: the agents' API mounted next to a route of its own.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Agent from "../../src/Agent.ts";
import * as Api from "../../src/Api.ts";
import { Echo, Flaky, Relay } from "./agents.ts";
import { Desk } from "./llm.ts";

export default Cloudflare.Worker(
  "AgentTestApi",
  { main: import.meta.url },
  Effect.gen(function* () {
    const agents = yield* Agent.connect(Echo, Flaky, Relay, Desk);
    const api = yield* Api.routes({ agents: [Echo, Flaky, Relay, Desk] });
    return {
      fetch: Effect.gen(function* () {
        const fromApi = yield* api.handle;
        if (fromApi) return fromApi;
        // A route of the Worker's own, calling an agent through a reference.
        const request = yield* HttpServerRequest;
        const shout = new URL(request.url, "http://local").pathname.match(/^\/shout\/([^/]+)$/);
        if (shout) {
          const out = yield* Echo.named("shouter").echo({ text: decodeURIComponent(shout[1]!) });
          return yield* HttpServerResponse.json(out).pipe(Effect.orDie);
        }
        return HttpServerResponse.text("Not found", { status: 404 });
      }).pipe(
        Effect.provide(agents),
        Effect.catchTags({
          TaskFailed: (e) => HttpServerResponse.json(e.error, { status: 500 }).pipe(Effect.orDie),
          AgentUnreachable: (e) => Effect.succeed(HttpServerResponse.text(e.message, { status: 503 })),
        }),
      ),
    };
  }),
);
