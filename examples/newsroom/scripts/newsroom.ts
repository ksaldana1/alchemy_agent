// The newsroom, end to end, against a deployed stage (M8; docs/NORTH-STAR.md's demo). From the repo root:
//   bun run newsroom:run <url>
// Uploads three submissions (and replays each storage event, as a duplicate delivery would), plays
// the person who signs off, and prints what each run did. The token comes from AGENT_API_TOKEN.
import { Platform } from "alchemy-agent";
import * as Effect from "effect/Effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as Stream from "effect/Stream";
import { Editor, Reviewer } from "../src/contracts.ts";
import { PublishArticle } from "../src/publish.ts";

const [url] = process.argv.slice(2);
const token = process.env.AGENT_API_TOKEN;
if (!url || !token) throw new Error("usage: newsroom.ts <url>  (needs AGENT_API_TOKEN)");

const batch = Date.now().toString(36);
const ARTICLES: Record<string, string> = {
  "bike-lanes": `City council aproves new bike lanes

The city council voted 7 to 2 on tuesday to aprove a network of protected bike lanes downtown. The project,
which will cost about $4.2 million, is expected to be finished by the end of next year.`,
  library: `Library to reopen in march

The central library will reopen in march after a two-year renovation. The city aproved the final $12 million
budget last week, and the library says it's collection have grown by 20,000 books.`,
  "school-budget": `School board passes budget

The school board aproves a budget of $48 million for next year, up 3 percent. Most of the increase goes to
teacher salarys, the board said.`,
};

const program = Effect.gen(function* () {
  const newsroom = yield* Platform.Client;
  const t0 = Date.now();
  const at = () => `${((Date.now() - t0) / 1000).toFixed(1)}s`;

  // Play the person: approve each sign-off request as it appears.
  yield* newsroom.requests.stream.pipe(
    Stream.take(Object.keys(ARTICLES).length),
    Stream.runForEach((request) =>
      Effect.gen(function* () {
        console.log(`${at()}  request ${request.name}: ${request.prompt}`);
        yield* request.respond({ approved: true, notes: "Ship it." }, { by: "editor-in-chief" });
      }),
    ),
    Effect.forkScoped,
  );

  for (const [name, text] of Object.entries(ARTICLES)) {
    const ref = yield* newsroom.files.write(`shared/submissions/${batch}-${name}.txt`, text);
    // A duplicate of the storage event R2 sends for this upload.
    yield* newsroom.listeners.replay({ action: "PutObject", object: { key: ref.path, size: ref.size, eTag: ref.revision } });
    console.log(`${at()}  uploaded ${ref.path}`);
  }

  const runs = yield* newsroom.runs(PublishArticle).pipe(Stream.take(Object.keys(ARTICLES).length), Stream.runCollect);
  console.log(`${at()}  ${runs.length} runs started`);
  for (const run of runs) {
    const published = yield* run.result;
    const [edit] = (yield* run.tasksOn(Editor.address("desk"))).filter((t) => t.task === "Edit");
    const [junior] = yield* run.tasksOn(Reviewer.address("junior"));
    const [senior] = yield* run.tasksOn(Reviewer.address("senior"));
    console.log(
      `${at()}  ${published.path}: edit attempts ${edit?.attempts}, junior ${junior?.status}` +
        ` (${(junior?.error as any)?.value?._tag ?? "-"}), senior ${(senior?.result as any)?.approved ? "approved" : senior?.status}`,
    );
  }
  const first = Object.keys(ARTICLES)[0]!;
  console.log(`\n${yield* newsroom.files.read(`shared/published/${batch}-${first}.md`)}`);
});

await Effect.runPromise(
  program.pipe(Effect.provide(Platform.connect({ url, token })), Effect.provide(FetchHttpClient.layer), Effect.scoped),
);
