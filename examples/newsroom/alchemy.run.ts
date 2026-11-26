import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import Newsroom from "./src/platform.ts";

export default Alchemy.Stack(
  "Newsroom",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    // Provisions the agents' Durable Objects, the workflow engine, the workspace, and the API Worker.
    const newsroom = yield* Newsroom;
    return { url: newsroom.url };
  }),
);
