import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import HelloApi from "./src/api.ts";

export default Alchemy.Stack(
  "Hello",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const api = yield* HelloApi;
    return { url: api.url };
  }),
);
