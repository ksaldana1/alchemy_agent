// How the newsroom's agents do their work (docs/NORTH-STAR.md), on Workers AI.
import { Agent, AgentModel, Workspace } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { Editor, Intake, Reviewer, Verdict } from "./contracts.ts";

const Model = AgentModel.workersAI("@cf/meta/llama-3.3-70b-instruct-fp8-fast");

export const IntakeLive = Intake.toLayer(
  Intake.of({
    File: Effect.fn("Intake.File")(function* ({ payload: { from, to } }) {
      const text = yield* Workspace.read(from);
      return yield* Workspace.write(to, text);
    }),
    Publish: ({ payload: { from, name } }) => Workspace.copy(from, `shared/published/${name}.md`),
  }),
);

export const EditorLive = Editor.toLayer(
  Editor.of({
    // One durable LLM turn; the handler saves its answer to the editor's home.
    Edit: Effect.fn("Editor.Edit")(function* ({ payload: { path, name } }) {
      const text = yield* Workspace.read(path);
      const edited = yield* Agent.generate({
        system:
          "You are a copy editor. Fix grammar and spelling and tighten the prose. Never change facts. " +
          "Reply with the edited article only, without any commentary.",
        prompt: text,
      });
      return yield* Workspace.write(`~/edited/${name}.md`, edited.trim());
    }),
    Share: ({ payload }) => Workspace.share(payload.path, payload.with, "read"),
  }),
).pipe(Layer.provide(Model));

export const ReviewerLive = Reviewer.toLayer(
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
).pipe(Layer.provide(Model));
