// The publish workflow (docs/NORTH-STAR.md): plain `effect/workflow`. Each agent call is a durable
// step: it runs once, and the run waits for the answer without holding execution.
import { Human, Workspace } from "alchemy-agent";
import * as Workflows from "alchemy/Workflows";
import type * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Workflow } from "effect/workflow";
import { Editor, Intake, Reviewer } from "./contracts.ts";

export class Rejected extends Schema.TaggedError<Rejected>()("Rejected", {
  by: Schema.String,
  notes: Schema.String,
}) {}

export const PublishArticle = Workflow.make("PublishArticle", {
  payload: {
    path: Schema.String,
    name: Schema.String,
    /** How long the sign-off may take. Default 2 days. */
    signOffWithin: Schema.optionalKey(Schema.String),
  },
  success: Workspace.FileRef,
  error: Rejected,
  idempotencyKey: ({ path }) => path,
});

export const PublishArticleLive = Workflows.layer(
  PublishArticle,
  Effect.fn(function* ({ path, name, signOffWithin }) {
    const intake = (yield* Intake.client)("intake");
    const desk = (yield* Editor.client)("desk");
    const reviewers = yield* Reviewer.client;

    const filed = yield* intake.File({ from: path, to: `shared/desk/${name}.md` }).pipe(
      Effect.catchTag("FsDenied", (e) => new Rejected({ by: "intake", notes: e.message })),
    );
    const edited = yield* desk.Edit({ path: filed.path, name });
    // Only the senior reviewer gets access; the junior reviewer's review is denied.
    yield* desk.Share({ path: edited.path, with: Reviewer.address("senior") });

    // Both reviews at once. A denial is a declared error: it comes back as itself.
    const reviews = yield* Effect.all(
      ["junior", "senior"].map((who) =>
        reviewers(who).Review({ path: edited.path, name }).pipe(Effect.catchTag("FsDenied", () => Effect.succeed(null))),
      ),
      { concurrency: "unbounded" },
    );
    const done = reviews.filter((r) => r !== null);
    if (done.length === 0) return yield* new Rejected({ by: "nobody", notes: "no reviewer could read the article" });
    const rejection = done.find((r) => !r.approved);
    if (rejection) return yield* new Rejected({ by: rejection.by, notes: rejection.notes });
    const review = done[0]!;

    // A person signs off on exactly the revision that was reviewed (GET /requests, then
    // POST /requests/:id/respond). A sign-off for another revision is rejected by the platform.
    yield* Human.approval("sign-off", {
      artifact: review.path,
      revision: review.revision,
      prompt: `Publish "${name}"? ${review.by} approved it: ${review.notes}`,
      timeout: (signOffWithin ?? "2 days") as Duration.Input,
    }).pipe(
      Effect.catchTags({
        NotApproved: (e) => new Rejected({ by: e.by, notes: e.notes || "not signed off" }),
        RequestExpired: () => new Rejected({ by: "timeout", notes: "not signed off in time" }),
        RequestCanceled: (e) => new Rejected({ by: e.by, notes: `sign-off canceled: ${e.reason}` }),
      }),
    );

    // Intake copies the edited file into the shared area, so it needs to read it too.
    yield* desk.Share({ path: edited.path, with: Intake.address("intake") });
    return yield* intake.Publish({ from: edited.path, name });
  },
  // A platform failure (retries exhausted, a denial nobody declared, …) ends the run, saying why.
  Effect.catchTag("AgentError", (e) => Effect.die(new Error(`${e.agent.type}/${e.agent.name}: ${JSON.stringify(e.reason)}`)))),
);
