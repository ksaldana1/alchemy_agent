// The platform: everything a set of agents and workflows needs, hosted in one Worker (M5).
//
//   // src/platform.ts
//   export default Platform.make("Newsroom", {
//     main: import.meta.url,
//     agents: [IntakeLive, EditorLive, ReviewerLive],
//     workflows: [PublishArticleLive],
//     grants: [Grant.read(Editor.address("desk"), "shared/desk/")],
//   });
//   // alchemy.run.ts: const newsroom = yield* Newsroom; return { url: newsroom.url };
//
// It hosts the agents' Durable Objects, the workflows on Alchemy's Durable Object engine (D9), the
// workspace when grants are declared, the listeners (M6), and the HTTP API (`Api.routes`: agents, workflow runs and
// signals, the workspace). The engine is given to the agents too, so their replies reach runs.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import type { WorkflowEngine } from "effect/workflow";
import * as Api from "./Api.ts";
import * as Listeners from "./internal/listeners.ts";
import { trackRegistrations } from "./internal/workflow.ts";
import type * as Listen from "./Listen.ts";
import * as Workspace from "./Workspace.ts";

export interface Options {
  /** Agent implementations: `X.toLayer(...)`. */
  readonly agents: ReadonlyArray<Layer.Layer<any, never, any>>;
  /** Workflow implementations: `Workflows.layer(W, ...)` or `W.toLayer(...)`. */
  readonly workflows?: ReadonlyArray<Layer.Layer<any, never, any>>;
  /** Platform grants on the workspace. Declaring any hosts the workspace. */
  readonly grants?: ReadonlyArray<Workspace.Grant>;
  /** Host the workspace even without grants, and how. */
  readonly workspace?: { readonly forceDestroy?: boolean };
  /** Listeners: `Listen.files(...)` / `Listen.cron(...)`, each `.start(workflow, …)` or `.send(agent, …)`. */
  readonly listeners?: ReadonlyArray<Listen.Listener>;
}

/**
 * Everything in `options` as one Layer, for a Worker you write yourself. `Platform.make` is this
 * plus the Worker and its HTTP API.
 */
export const layer = (options: Options): Layer.Layer<WorkflowEngine.WorkflowEngine, never, Cloudflare.Worker> => {
  const engine = trackRegistrations(Cloudflare.Workflows.layerDurableObjectEngine());
  const parts: Array<Layer.Layer<any, never, any>> = [...options.agents, ...(options.workflows ?? [])];
  const listeners = options.listeners ?? [];
  const forceDestroy = options.workspace?.forceDestroy ?? false;
  // File listeners watch the workspace's bucket, so they host it too.
  const hasFileListeners = listeners.some((l) => l.source.kind === "files");
  if (options.grants !== undefined || options.workspace !== undefined || hasFileListeners) {
    parts.push(Workspace.layer({ grants: options.grants ?? [], forceDestroy }));
  }
  if (listeners.length > 0) parts.push(Listeners.layer(listeners, { forceDestroy }) as Layer.Layer<any, never, any>);
  // provideMerge: the agents and workflows are built with the engine in their context, and the
  // engine stays available to the Worker (to start runs and send signals).
  return Layer.mergeAll(...(parts as [Layer.Layer<any, never, any>])).pipe(Layer.provideMerge(engine)) as never;
};

/** A Worker hosting the platform and serving its HTTP API (see `Api`). Yield it in your stack. */
export const make = (name: string, options: Options & { readonly main: string }) =>
  Cloudflare.Worker(
    name,
    { main: options.main },
    Effect.gen(function* () {
      const api = yield* Api.routes();
      return {
        fetch: Effect.gen(function* () {
          return (yield* api.handle) ?? HttpServerResponse.text("Not found", { status: 404 });
        }),
      };
    }).pipe(Effect.provide(layer(options))),
  );
