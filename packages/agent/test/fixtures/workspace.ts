// Workspace test agents (M4): `Filer` exercises each operation from a handler; `Librarian` is an
// LLM agent (scripted model) using the workspace tools.
import * as Prompt from "effect/ai/Prompt";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";
import * as Agent from "../../src/Agent.ts";
import * as AgentModel from "../../src/AgentModel.ts";
import * as Grant from "../../src/Grant.ts";
import * as Workspace from "../../src/Workspace.ts";

// ---------------------------------------------------------------------------
// Contracts
// ---------------------------------------------------------------------------

const Access = Workspace.AccessSchema;

export const Write = Rpc.make("Write", { payload: { path: Schema.String, content: Schema.String }, success: Workspace.FileRef });
/** Doesn't declare FsDenied: a denial fails the task with AgentError(FsDenied), without retrying. */
export const Read = Rpc.make("Read", { payload: { path: Schema.String }, success: Schema.String });
/** Declares FsDenied: the caller gets it as itself. */
export const TryRead = Rpc.make("TryRead", { payload: { path: Schema.String }, success: Schema.String, error: Workspace.FsDenied });
export const List = Rpc.make("List", { payload: { path: Schema.String }, success: Schema.Array(Schema.String), error: Workspace.FsDenied });
export const Share = Rpc.make("Share", {
  payload: { path: Schema.String, to: Agent.Address, access: Access },
  error: Workspace.FsDenied,
});
export const Revoke = Rpc.make("Revoke", { payload: { path: Schema.String, from: Agent.Address }, error: Workspace.FsDenied });
export const Copy = Rpc.make("Copy", { payload: { from: Schema.String, to: Schema.String }, success: Workspace.FileRef, error: Workspace.FsDenied });
export const Revision = Rpc.make("Revision", { payload: { path: Schema.String }, success: Schema.String, error: Workspace.FsDenied });
export const Filer = Agent.make("Filer", [Write, Read, TryRead, List, Share, Revoke, Copy, Revision]);

export const Summarize = Rpc.make("Summarize", { payload: { path: Schema.String, shareWith: Schema.String }, success: Schema.String });
export const Peek = Rpc.make("Peek", { payload: { path: Schema.String }, success: Schema.String });
export const Librarian = Agent.make("Librarian", [Summarize, Peek]);

/** Platform grants for the tests. Agent names here are fixed; files are per run. */
export const grants = [
  Grant.read(Filer.address("alice"), "shared/documents/"),
  Grant.write(Filer.address("alice"), "shared/artifacts"),
  Grant.read(Filer.address("alice"), "shared/artifacts/locked"),
  Grant.read(Librarian.address("lib"), "shared/docs"),
];

// ---------------------------------------------------------------------------
// Implementations
// ---------------------------------------------------------------------------

export const FilerLive = Filer.toLayer(
  Filer.of({
    Write: ({ payload }) => Workspace.write(payload.path, payload.content),
    Read: ({ payload }) => Workspace.read(payload.path),
    TryRead: ({ payload }) => Workspace.read(payload.path),
    List: ({ payload }) => Workspace.list(payload.path),
    Share: ({ payload }) => Workspace.share(payload.path, payload.to, payload.access),
    Revoke: ({ payload }) => Workspace.revoke(payload.path, payload.from),
    Copy: ({ payload }) => Workspace.copy(payload.from, payload.to),
    Revision: ({ payload }) => Workspace.revision(payload.path),
  }),
  { retry: { maxAttempts: 3, delay: "200 millis" } },
);

/** The last user message, how many assistant messages answered it, and the whole prompt as JSON. */
const latest = (prompt: Prompt.Prompt) => {
  const msgs = prompt.content;
  let i = msgs.length - 1;
  while (i >= 0 && msgs[i]!.role !== "user") i--;
  const user = msgs[i];
  const text = user === undefined ? "" : typeof user.content === "string" ? user.content : user.content.map((p) => ("text" in p ? p.text : "")).join("");
  return { text, n: msgs.slice(i + 1).filter((m) => m.role === "assistant").length, all: JSON.stringify(msgs) };
};

// "summarize <path> for <name>": read it, write ~/summary.md, share it with Filer/<name>, answer.
// "peek <path>": read it, then report whether the tool was denied.
const script = (prompt: Prompt.Prompt): AgentModel.ScriptedResponse => {
  const { text, n, all } = latest(prompt);
  const summarize = text.match(/^summarize (\S+) for (\S+)$/);
  if (summarize) {
    const [, path, name] = summarize;
    if (n === 0) return AgentModel.toolCall("read_file", { path });
    if (n === 1) return AgentModel.toolCall("write_file", { path: "~/summary.md", content: `Summary of ${path}: ${all.includes("quarterly") ? "quarterly numbers" : "?"}` });
    if (n === 2) return AgentModel.toolCall("share_file", { path: "~/summary.md", agentType: "Filer", agentName: name!, access: "read" });
    return AgentModel.text(all.includes("FsDenied") ? "denied" : "summarized and shared");
  }
  const peek = text.match(/^peek (\S+)$/);
  if (peek) return n === 0 ? AgentModel.toolCall("read_file", { path: peek[1]! }) : AgentModel.text(all.includes("FsDenied") ? "the read was denied" : "read it");
  return AgentModel.text("?");
};

export const LibrarianLive = Librarian.toLayer(
  Librarian.of({
    Summarize: ({ payload }) => Agent.generate({ prompt: `summarize ${payload.path} for ${payload.shareWith}` }),
    Peek: ({ payload }) => Agent.generate({ prompt: `peek ${payload.path}` }),
  }),
  { toolkit: Workspace.Toolkit },
).pipe(Layer.provide([AgentModel.scripted(script), Workspace.ToolkitLayer]));

export const WorkspaceLive = Workspace.layer({ grants, forceDestroy: true });

export const WorkspaceAgentsLive = Layer.mergeAll(FilerLive, LibrarianLive, WorkspaceLive);
