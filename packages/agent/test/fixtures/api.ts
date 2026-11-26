// The test Worker: hosts the agents (their Layers) and mounts their API next to a route of its own.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import * as Api from "../../src/Api.ts";
import { Echo, EchoLive, Flaky, FlakyLive, Relay, RelayLive } from "./agents.ts";
import { Desk, DeskLive } from "./llm.ts";
import { Filer, Librarian, WorkspaceAgentsLive } from "./workspace.ts";

export default Cloudflare.Worker(
  "AgentTestApi",
  { main: import.meta.url },
  Effect.gen(function* () {
    const api = yield* Api.routes([Echo, Flaky, Relay, Desk, Filer, Librarian]);
    const echoes = yield* Echo.client;
    return {
      fetch: Effect.gen(function* () {
        const fromApi = yield* api.handle;
        if (fromApi) return fromApi;
        // A route of the Worker's own, calling an agent through its client.
        const request = yield* HttpServerRequest;
        const shout = new URL(request.url, "http://local").pathname.match(/^\/shout\/([^/]+)$/);
        if (shout) {
          const out = yield* echoes("shouter").EchoText({ text: decodeURIComponent(shout[1]!) });
          return yield* HttpServerResponse.json(out).pipe(Effect.orDie);
        }
        return HttpServerResponse.text("Not found", { status: 404 });
      }).pipe(
        Effect.catchTag("AgentError", (e) => Effect.succeed(HttpServerResponse.text(e.message, { status: 503 }))),
      ),
    };
  }).pipe(Effect.provide(Layer.mergeAll(EchoLive, FlakyLive, RelayLive, DeskLive, WorkspaceAgentsLive))),
);
