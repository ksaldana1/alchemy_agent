// The demo's stack (scripted model, planned failures). `bun run refunds:demo`.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import RefundsDemo from "./platform.ts";

export default Alchemy.Stack(
  "RefundsDemo",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const refunds = yield* RefundsDemo;
    return { url: refunds.url };
  }),
);
