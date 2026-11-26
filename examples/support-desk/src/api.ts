import * as Cloudflare from "@alchemy.run/cloudflare";
import { Agent, Api } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { Helpdesk, Responder, Triage } from "./agents.ts";

export default Cloudflare.Worker(
  "SupportDeskApi",
  { main: import.meta.url },
  Effect.gen(function* () {
    const agents = yield* Agent.connect(Triage, Responder, Helpdesk);
    const api = yield* Api.routes({ agents: [Triage, Responder, Helpdesk] });
    return {
      fetch: Effect.gen(function* () {
        return (yield* api.handle) ?? HttpServerResponse.text("Support desk.");
      }).pipe(Effect.provide(agents)),
    };
  }),
);
