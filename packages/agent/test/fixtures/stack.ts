import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import AgentTestApi from "./api.ts";

export default Alchemy.Stack(
  "AgentTest",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const api = yield* AgentTestApi;
    return { url: api.url };
  }),
);
