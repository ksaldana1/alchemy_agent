// The newsroom under test: the same agents, workflow, grants, and listener, on a scripted model,
// with a planned crash. Chaos lives here, never in the app code.
import { AgentModel, Platform } from "alchemy-agent";
import * as Testing from "alchemy-agent/testing";
import * as Layer from "effect/Layer";
import { EditorAgent, IntakeLive, ReviewerAgent } from "../src/agents.ts";
import { Editor } from "../src/contracts.ts";
import { grants, listeners } from "../src/platform.ts";
import { PublishArticleLive } from "../src/publish.ts";
import { script } from "./model.ts";

const Model = AgentModel.scripted(script);

export default Platform.make("NewsroomTestApi", {
  main: import.meta.url,
  agents: [IntakeLive, EditorAgent.pipe(Layer.provide(Model)), ReviewerAgent.pipe(Layer.provide(Model))],
  workflows: [PublishArticleLive],
  grants,
  listeners,
  workspace: { forceDestroy: true },
  // The editor dies on its first edit (as an eviction would); the edit is delivered again.
  provide: Testing.chaos([Testing.crashOnce(Editor, "Edit")]),
});
