// The newsroom platform: agents, the publish workflow, the workspace grants, and the listener, in one Worker.
import { Grant, Listen, Platform } from "alchemy-agent";
import { EditorLive, IntakeLive, ReviewerLive } from "./agents.ts";
import { Editor, Intake } from "./contracts.ts";
import { PublishArticle, PublishArticleLive } from "./publish.ts";

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
  // Exactly one run per uploaded file, even if the storage event is delivered twice: the run's key
  // is the file's path (PublishArticle's idempotencyKey).
  listeners: [
    Listen.files("shared/submissions/", { suffix: ".txt" }).start(PublishArticle, (file) => ({ path: file.path, name: file.basename })),
  ],
  workspace: { forceDestroy: true },
});
