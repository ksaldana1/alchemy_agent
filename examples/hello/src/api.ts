import * as Cloudflare from "@alchemy.run/cloudflare";
import { Agent, Api } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { Greeter } from "./greeter.ts";

export default Cloudflare.Worker(
  "HelloApi",
  { main: import.meta.url },
  Effect.gen(function* () {
    const agents = yield* Agent.connect(Greeter);
    const api = yield* Api.routes({ agents: [Greeter] });
    return {
      fetch: Effect.gen(function* () {
        return (yield* api.handle) ?? HttpServerResponse.text("Hello from the Hello example.");
      }).pipe(Effect.provide(agents)),
    };
  }),
);
