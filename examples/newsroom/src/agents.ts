// How the newsroom's agents do their work (docs/NORTH-STAR.md). The LLM agents' Layers need a
// model: `*Live` provides Workers AI; the chaos test provides a scripted model instead.
import { Agent, AgentModel, Workspace } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Editor, Intake, Reviewer, Verdict } from "./contracts.ts";

export const IntakeLive = Intake.toLayer(
  Intake.of({
    File: Effect.fn("Intake.File")(function* ({ payload: { from, to } }) {
      const text = yield* Workspace.read(from);
      return yield* Workspace.write(to, text);
    }),
    Publish: ({ payload: { from, name } }) => Workspace.copy(from, `shared/published/${name}.md`),
  }),
);

export const EditorAgent = Editor.toLayer(
  Editor.of({
    // One durable LLM turn: the model reads the article and writes the edit with its workspace
    // tools. Every model call and tool call is a checkpointed step.
    Edit: Effect.fn("Editor.Edit")(function* ({ payload: { path, name } }) {
      const target = `~/edited/${name}.md`;
      const answer = yield* Agent.generate({
        system:
          "You are a copy editor. Fix grammar and spelling and tighten the prose. Never change facts. " +
          "Use read_file to read the article and write_file to save your edited version.",
        prompt: `Edit ${path} and save the edited article to ${target}. Then reply "done".`,
      });
      // A model that answered with the article instead of saving it: save its answer.
      return yield* Workspace.stat(target).pipe(Effect.catchTag("FileNotFound", () => Workspace.write(target, answer.trim())));
    }),
    Share: ({ payload }) => Workspace.share(payload.path, payload.with, "read"),
  }),
  // The agent's tools (read / write / list / share, checked against its grants).
  { toolkit: Workspace.Toolkit },
).pipe(Layer.provide(Workspace.ToolkitLayer));

export const ReviewerAgent = Reviewer.toLayer(
  Reviewer.of({
    Review: Effect.fn("Reviewer.Review")(function* ({ payload: { path }, agent }) {
      // Without a grant this fails with FsDenied: a declared error, so the task settles.
      const text = yield* Workspace.read(path);
      const verdict = yield* Agent.generate({
        system:
          "You review edited news articles for accuracy, clarity, and tone. Approve unless something is clearly wrong. " +
          "Keep the notes to one or two sentences.",
        prompt: `Review this article:\n\n${text}`,
        schema: Verdict,
      });
      return { ...verdict, by: agent.name, path, revision: yield* Workspace.revision(path) };
    }),
  }),
);

const Model = AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast");
export const EditorLive = EditorAgent.pipe(Layer.provide(Model));
export const ReviewerLive = ReviewerAgent.pipe(Layer.provide(Model));
