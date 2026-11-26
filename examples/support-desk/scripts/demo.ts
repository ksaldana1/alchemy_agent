// Support desk against a deployed stage. Run from the repo root:
//   bun run support:demo <url> tickets                 triage → respond, five tickets
//   bun run support:demo <url> chat <session> <text>   one chat turn with tools; prints the steps
//   bun run support:demo <url> events <session>        the session's SSE stream
//   bun run support:demo <url> redeploy-start          starts a turn whose tool takes ~15 s
//   bun run support:demo <url> redeploy-finish <ref>   waits for it after a redeploy
// The token comes from AGENT_API_TOKEN (examples/support-desk/.env).
import { Api } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Schedule from "effect/Schedule";
import { Helpdesk, Responder, Triage } from "../src/contracts.ts";

const [url, step, ...rest] = process.argv.slice(2);
const token = process.env[Api.TOKEN_ENV];
if (!url || !step || !token) {
  console.error("usage: demo.ts <url> tickets|chat|events|redeploy-start|redeploy-finish …  (needs AGENT_API_TOKEN)");
  process.exit(1);
}
const run = Date.now().toString(36);

const TICKETS: Record<string, string> = {
  "double-charge": "Hi, I was charged twice for my Pro plan this month (two $49 charges on the 3rd). Can you fix this?",
  "export-broken": "CSV export has been spinning forever since yesterday's update. I need this report for a board meeting in 2 hours!!",
  "dark-mode": "Would love a dark mode for the dashboard. My eyes thank you in advance.",
  chargeback: "This is the third time your service went down. If I don't hear back today I'm filing a chargeback and talking to my lawyer.",
  "leaked-key": "My integration fails with 401. Here's my key so you can test: sk-live-9f8a7b6c5d4e3f2a1b0c. What am I doing wrong?",
};

const get = (path: string) =>
  Effect.promise(() => fetch(`${url}${path}`, { headers: { authorization: `Bearer ${token}` } }).then((r) => r.text()));

type Entry = { kind: string; message: { content: unknown } };
const printSteps = (entries: ReadonlyArray<Entry>) => {
  for (const e of entries) {
    const c = e.message.content;
    const parts = typeof c === "string" ? [{ type: "text", text: c }] : (c as Array<Record<string, unknown>>);
    for (const p of parts) {
      if (p.type === "text" && p.text) console.log(`  ${e.kind.padEnd(11)} text: ${String(p.text).replace(/\n/g, " ").slice(0, 140)}`);
      if (p.type === "tool-call") console.log(`  ${e.kind.padEnd(11)} tool-call ${p.name} ${JSON.stringify(p.params)}`);
      if (p.type === "tool-result") console.log(`  ${e.kind.padEnd(11)} tool-result ${p.name} ${p.isFailure ? "FAILED " : ""}${JSON.stringify(p.result)}`);
    }
  }
};

const program = Effect.gen(function* () {
  const helpdesk = (yield* Helpdesk.client)("front");
  if (step === "tickets") {
    const triage = (yield* Triage.client)(`front-${run}`);
    const responder = (yield* Responder.client)("desk");
    const rows = yield* Effect.forEach(
      Object.entries(TICKETS),
      ([name, ticket]) =>
        Effect.gen(function* () {
          const key = `${run}-${name}`;
          const t = yield* triage.TriageTicket({ name: key, ticket });
          // Same key, same task: this waits for the one triage already sent.
          const r = yield* responder.Respond({ name: key, ticket, triage: t });
          return {
            ticket: name,
            category: t.category,
            urgency: t.urgency,
            human: t.needsHuman,
            secret: t.hasSecret,
            action: r.action,
            draft: r.draft ? `${r.draft.replace(/\n/g, " ").slice(0, 60)}…` : r.reasons.join(", "),
          };
        }),
      { concurrency: "unbounded" },
    );
    console.table(rows);
  } else if (step === "chat") {
    const [session = `chat-${run}`, ...words] = rest;
    const message = words.join(" ") || "What is the status of order 42?";
    const t0 = Date.now();
    const answer = yield* helpdesk.Chat({ message, session });
    console.log(`answer (${Date.now() - t0} ms): ${answer}\nsteps:`);
    printSteps(JSON.parse(yield* get(`/agents/Helpdesk/front/sessions/${session}`)));
  } else if (step === "events") {
    console.log(yield* get(`/agents/Helpdesk/front/sessions/${rest[0]}/events`));
  } else if (step === "redeploy-start") {
    const session = `redeploy-${run}`;
    yield* helpdesk.Chat({ message: "How many blue mugs are in stock at the warehouse?", session }, { discard: true });
    // Wait until the slow tool has been called, so the redeploy lands mid-turn.
    yield* get(`/agents/Helpdesk/front/sessions/${session}`).pipe(
      Effect.repeat({ schedule: Schedule.spaced("500 millis"), until: (t) => t.includes("check_warehouse"), times: 60 }),
    );
    console.log(`mid-turn: the model called check_warehouse (~15 s). Redeploy now, then:`);
    console.log(`bun run support:demo ${url} redeploy-finish ${session}`);
  } else if (step === "redeploy-finish") {
    const session = rest[0]!;
    // Wait on the session's events until it's idle, then show the transcript.
    yield* get(`/agents/Helpdesk/front/sessions/${session}/events`);
    console.log("the turn finished after the redeploy. steps:");
    printSteps(JSON.parse(yield* get(`/agents/Helpdesk/front/sessions/${session}`)));
  }
});

await Effect.runPromise(
  program.pipe(Effect.provide(Api.connect({ url, token })), Effect.provide(FetchHttpClient.layer)),
);
