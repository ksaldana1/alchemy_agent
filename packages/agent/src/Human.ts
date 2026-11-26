// Human requests (M7): a durable question for a person, with a typed answer, an expiry, and
// (optionally) the exact file revision it is about.
//
//   // in a workflow, a handler, or a tool:
//   yield* Human.approval("sign-off", { artifact: review.path, revision: review.revision, timeout: "2 days" })
//   const budget = yield* Human.request("budget", { schema: Schema.Struct({ amount: Schema.Number }), timeout: "1 day" })
//
//   // a person, over the API:
//   GET  /requests                       the pending requests (each with its prompt, schema, and artifact)
//   POST /requests/:id/respond { response, by, revision? }
//
// A request is a task on the built-in `AgentHumans` agent that stays waiting until a person
// answers, the timeout passes (`RequestExpired`), or someone cancels it (`RequestCanceled`). So it
// waits like any agent call: a durable step in a workflow, a waiting call in a handler or tool,
// holding no execution either way. The first answer settles it; a duplicate is a no-op. An
// answer for an artifact must name the revision the request is bound to, and that must still be
// the file's current revision. Otherwise it is stale and rejected.
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Rpc } from "effect/rpc";
import * as Agent from "./Agent.ts";
import { ExternalTask } from "./internal/core.ts";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/** Nobody answered before the request's timeout. */
export class RequestExpired extends Schema.TaggedError<RequestExpired>()("RequestExpired", {
  request: Schema.String,
  expiredAt: Schema.Number,
}) {}

/** The request was canceled over the API. */
export class RequestCanceled extends Schema.TaggedError<RequestCanceled>()("RequestCanceled", {
  request: Schema.String,
  by: Schema.String,
  reason: Schema.String,
}) {}

/** An approval was answered with a no. */
export class NotApproved extends Schema.TaggedError<NotApproved>()("NotApproved", {
  request: Schema.String,
  by: Schema.String,
  notes: Schema.String,
}) {}

// ---------------------------------------------------------------------------
// The AgentHumans agent (built in; `Platform.layer` hosts it)
// ---------------------------------------------------------------------------

/** The file revision a request is about. */
export const Artifact = Schema.Struct({ path: Schema.String, revision: Schema.String });

/** A person's answer, as the request's task records it. */
export const Answer = Schema.Struct({
  response: Schema.Unknown,
  by: Schema.String,
  at: Schema.Number,
  /** For a request about an artifact: the revision the person answered for. */
  revision: Schema.NullOr(Schema.String),
});
export type Answer = typeof Answer.Type;

export const Ask = Rpc.make("Ask", {
  payload: {
    name: Schema.String,
    prompt: Schema.String,
    /** JSON Schema of the response; the API checks answers against it. */
    schema: Schema.Unknown,
    artifact: Schema.NullOr(Artifact),
    expiresAt: Schema.NullOr(Schema.Number),
  },
  success: Answer,
  error: Schema.Union([RequestExpired, RequestCanceled]),
});

export const Humans = Agent.make("AgentHumans", [Ask]);
/** All requests are tasks of one instance. */
export const PEOPLE = "people";

export const HumansLive = Humans.toLayer(
  Humans.of({
    // Waits until a person answers (the `resolve` RPC settles the task), or until it expires: the
    // task is resumed at `expiresAt`, and this fails it.
    Ask: ({ payload, taskId }) =>
      Effect.gen(function* () {
        if (payload.expiresAt !== null && Date.now() >= payload.expiresAt) {
          return yield* new RequestExpired({ request: taskId, expiredAt: payload.expiresAt });
        }
        return yield* (yield* ExternalTask).waitUntil(payload.expiresAt);
      }),
  }),
  // Expiring and waiting aren't failures: never retry a request's task on its own.
  { retry: { maxAttempts: 1 } },
);

// ---------------------------------------------------------------------------
// Asking
// ---------------------------------------------------------------------------

export interface RequestOptions<S extends Schema.Codec<any, any, never, never>> {
  /** The response's schema. */
  readonly schema: S;
  /** What the person is asked. Default: the request's name. */
  readonly prompt?: string;
  /** The file revision the request is about: answers must name it, and it must still be current. */
  readonly artifact?: { readonly path: string; readonly revision: string };
  /** How long the person has. Default: no limit. */
  readonly timeout?: Duration.Input;
}

const ask = <S extends Schema.Codec<any, any, never, never>>(name: string, options: RequestOptions<S>) =>
  Effect.gen(function* () {
    const people = (yield* Humans.client)(PEOPLE);
    // In a workflow this payload is recorded with the step the first time, so the deadline
    // doesn't move when the run replays; in a handler the first recorded call wins the same way.
    const answer = yield* people.Ask({
      name,
      prompt: options.prompt ?? name,
      schema: Schema.toJsonSchemaDocument(options.schema),
      artifact: options.artifact ?? null,
      expiresAt: options.timeout === undefined ? null : Date.now() + Duration.toMillis(Duration.fromInputUnsafe(options.timeout)),
    });
    const response = yield* Schema.decodeUnknownEffect(options.schema)(answer.response).pipe(Effect.orDie);
    return { answer, response: response as S["Type"] };
  });

/** Asks a person, and waits for the typed answer. */
export const request = <S extends Schema.Codec<any, any, never, never>>(
  name: string,
  options: RequestOptions<S>,
): Effect.Effect<S["Type"], RequestExpired | RequestCanceled | Agent.AgentError> => Effect.map(ask(name, options), (a) => a.response);

export const Approval = Schema.Struct({ approved: Schema.Boolean, notes: Schema.optionalKey(Schema.String) });

/**
 * Asks a person to approve exactly this revision of a file. Succeeds with who approved it; a no
 * fails with `NotApproved`. An approval sent for an older revision is rejected by the API.
 */
export const approval = (
  name: string,
  options: { readonly artifact: string; readonly revision: string; readonly prompt?: string; readonly timeout?: Duration.Input },
): Effect.Effect<{ readonly by: string; readonly notes: string; readonly revision: string }, NotApproved | RequestExpired | RequestCanceled | Agent.AgentError> =>
  Effect.gen(function* () {
    const { answer, response } = yield* ask(name, {
      schema: Approval,
      prompt: options.prompt ?? `Approve ${options.artifact} (revision ${options.revision})?`,
      artifact: { path: options.artifact, revision: options.revision },
      ...(options.timeout === undefined ? {} : { timeout: options.timeout }),
    });
    if (!response.approved) return yield* new NotApproved({ request: name, by: answer.by, notes: response.notes ?? "" });
    return { by: answer.by, notes: response.notes ?? "", revision: options.revision };
  });
