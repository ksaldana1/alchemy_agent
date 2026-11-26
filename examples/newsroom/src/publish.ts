// The publish workflow (docs/NORTH-STAR.md): plain `effect/workflow`. Each agent call is a durable
// step: it runs once, and the run waits for the answer without holding execution.
import { Signal, Workspace } from "alchemy-agent";
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

/** A person's sign-off, sent over the API: POST /workflows/PublishArticle/runs/<id>/signals/sign-off. */
export const SignOff = Signal.make("sign-off", Schema.Struct({ approved: Schema.Boolean, by: Schema.String }));

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

    // A person signs off. No answer in time is a rejection; a late answer changes nothing.
    const signOff = yield* Signal.await(SignOff, { timeout: (signOffWithin ?? "2 days") as Duration.Input }).pipe(
      Effect.catchTag("SignalTimeout", () => Effect.succeed({ approved: false, by: "timeout" })),
    );
    if (!signOff.approved) return yield* new Rejected({ by: signOff.by, notes: "not signed off" });

    // Intake copies the edited file into the shared area, so it needs to read it too.
    yield* desk.Share({ path: edited.path, with: Intake.address("intake") });
    return yield* intake.Publish({ from: edited.path, name });
  },
  // A platform failure (retries exhausted, a denial nobody declared, …) ends the run, saying why.
  Effect.catchTag("AgentError", (e) => Effect.die(new Error(`${e.agent.type}/${e.agent.name}: ${JSON.stringify(e.reason)}`)))),
);
