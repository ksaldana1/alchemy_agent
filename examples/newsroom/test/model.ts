// A scripted model for the newsroom: the editor reads, writes a cleaned-up copy with its tools,
// and answers "done"; a reviewer approves.
import { AgentModel } from "alchemy-agent";
import type * as Prompt from "effect/ai/Prompt";

const latest = (prompt: Prompt.Prompt) => {
  const msgs = prompt.content;
  let i = msgs.length - 1;
  while (i >= 0 && msgs[i]!.role !== "user") i--;
  const user = msgs[i];
  const text = user === undefined ? "" : typeof user.content === "string" ? user.content : user.content.map((p) => ("text" in p ? p.text : "")).join("");
  return { text, after: msgs.slice(i + 1) };
};

/** The last tool result's value, as the model saw it. */
const lastToolResult = (after: ReadonlyArray<Prompt.Message>): unknown => {
  for (let i = after.length - 1; i >= 0; i--) {
    const m = after[i]!;
    if (m.role !== "tool") continue;
    const part = m.content.at(-1);
    if (part && "result" in part) return part.result;
  }
  return undefined;
};

export const script = (prompt: Prompt.Prompt): AgentModel.ScriptedResponse => {
  const { text, after } = latest(prompt);
  const steps = after.filter((m) => m.role === "assistant").length;
  const edit = text.match(/^Edit (\S+) and save the edited article to (\S+)\./);
  if (edit) {
    if (steps === 0) return AgentModel.toolCall("read_file", { path: edit[1]! });
    if (steps === 1) {
      const original = String(lastToolResult(after) ?? "");
      return AgentModel.toolCall("write_file", { path: edit[2]!, content: original.replace(/aproves?/g, (w) => w.replace("aprove", "approve")) });
    }
    return AgentModel.text("done");
  }
  if (text.startsWith("Review this article")) return AgentModel.respond({ approved: true, notes: "Reads well." });
  return AgentModel.text("?");
};
