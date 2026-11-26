// The newsroom's test stack (scripted model, planned crash). `bun run newsroom:test`.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import NewsroomTest from "./platform.ts";

export default Alchemy.Stack(
  "NewsroomTest",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const newsroom = yield* NewsroomTest;
    return { url: newsroom.url };
  }),
);
