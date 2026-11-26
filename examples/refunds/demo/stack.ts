// The demo's stack: the refund desk with a scripted model, crashes on demand, and a live page.
//   bun run refunds:ui     open it in a browser (alchemy dev, local)
//   bun run refunds:demo   the narrated run in the terminal, which is also the example's test
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
