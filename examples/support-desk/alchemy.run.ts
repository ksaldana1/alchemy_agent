import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import SupportDeskApi from "./src/api.ts";

export default Alchemy.Stack(
  "SupportDesk",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const api = yield* SupportDeskApi;
    return { url: api.url };
  }),
);
