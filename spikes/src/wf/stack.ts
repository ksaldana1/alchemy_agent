// Spike 0.4's stack: one Worker per workflow engine.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import WfDo from "./do.ts";
import WfNative from "./native.ts";

export default Alchemy.Stack(
  "SpikeWorkflows",
  { providers: Cloudflare.providers(), state: Cloudflare.state() },
  Effect.gen(function* () {
    const doWorker = yield* WfDo;
    const nativeWorker = yield* WfNative;
    return { doUrl: doWorker.url, nativeUrl: nativeWorker.url };
  }),
);
