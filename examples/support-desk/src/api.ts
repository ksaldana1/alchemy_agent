import * as Cloudflare from "@alchemy.run/cloudflare";
import { Api } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import { HelpdeskLive, ReaderLive, ResponderLive, ScribeLive, TriageLive, WorkspaceLive } from "./agents.ts";
import { Helpdesk, Reader, Responder, Scribe, Triage } from "./contracts.ts";

export default Cloudflare.Worker(
  "SupportDeskApi",
  { main: import.meta.url },
  Effect.gen(function* () {
    const api = yield* Api.routes([Triage, Responder, Helpdesk, Scribe, Reader]);
    return {
      fetch: Effect.gen(function* () {
        return (yield* api.handle) ?? HttpServerResponse.text("Support desk.");
      }),
    };
  }).pipe(Effect.provide(Layer.mergeAll(TriageLive, ResponderLive, HelpdeskLive, ScribeLive, ReaderLive, WorkspaceLive))),
);
