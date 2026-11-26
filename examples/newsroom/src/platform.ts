// The newsroom platform: agents, the publish workflow, and the workspace grants, in one Worker.
import { Grant, Platform } from "alchemy-agent";
import { EditorLive, IntakeLive, ReviewerLive } from "./agents.ts";
import { Editor, Intake } from "./contracts.ts";
import { PublishArticleLive } from "./publish.ts";

export default Platform.make("NewsroomApi", {
  main: import.meta.url,
  agents: [IntakeLive, EditorLive, ReviewerLive],
  workflows: [PublishArticleLive],
  // Only the platform grants access to the shared area. The editor shares its own files per article.
  grants: [
    Grant.read(Intake.address("intake"), "shared/submissions/"),
    Grant.write(Intake.address("intake"), "shared/desk/"),
    Grant.write(Intake.address("intake"), "shared/published/"),
    Grant.read(Editor.address("desk"), "shared/desk/"),
  ],
  workspace: { forceDestroy: true },
});
