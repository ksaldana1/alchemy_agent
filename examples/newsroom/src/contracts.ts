// What the newsroom's agents accept and return (docs/NORTH-STAR.md). No runtime code: the workflow
// and scripts import these to call the agents.
import { Agent, Workspace } from "alchemy-agent";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";

// Intake: files submissions on the desk, and publishes approved articles.
export const File = Rpc.make("File", {
  payload: { from: Schema.String, to: Schema.String },
  success: Workspace.FileRef,
  error: Workspace.FsDenied,
});
export const Publish = Rpc.make("Publish", {
  payload: { from: Schema.String, name: Schema.String },
  success: Workspace.FileRef,
});
export const Intake = Agent.make("Intake", [File, Publish]);

// Editor: an LLM agent that edits, then shares the result.
export const Edit = Rpc.make("Edit", {
  payload: { path: Schema.String, name: Schema.String },
  success: Workspace.FileRef,
});
export const Share = Rpc.make("Share", {
  payload: { path: Schema.String, with: Agent.Address },
});
export const Editor = Agent.make("Editor", [Edit, Share]);

// Reviewer: an LLM agent with a structured verdict. A missing grant is a declared error.
export const Verdict = Schema.Struct({ approved: Schema.Boolean, notes: Schema.String });
export const Review = Rpc.make("Review", {
  payload: { path: Schema.String, name: Schema.String },
  success: Schema.Struct({ ...Verdict.fields, by: Schema.String, path: Schema.String, revision: Schema.String }),
  error: Workspace.FsDenied,
});
export const Reviewer = Agent.make("Reviewer", [Review]);
