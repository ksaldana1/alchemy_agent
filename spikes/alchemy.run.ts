import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import SpikesApi from "./src/worker.ts";

export default Alchemy.Stack(
  "Spikes",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const api = yield* SpikesApi;
    return { url: api.url };
  }),
);
