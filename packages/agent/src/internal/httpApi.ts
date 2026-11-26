// The agents' HTTP API as an `HttpApi` definition: endpoints, schemas, and errors, with no server
// code, so clients (`Api.connect`, `Platform.connect`) and the server (`Api.routes`) share it.
// `Api.Definition` exports it, e.g. for an OpenAPI document or a client of your own.
import * as Schema from "effect/Schema";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiMiddleware, HttpApiSchema, HttpApiSecurity, OpenApi } from "effect/http-api";
import { TaskRecord } from "./inbox.ts";

// ---------------------------------------------------------------------------
// Errors. Each body keeps `error` (a short code or sentence) for people reading responses.
// ---------------------------------------------------------------------------

export class Unauthorized extends Schema.TaggedError<Unauthorized>()("Unauthorized", { error: Schema.String }, { httpApiStatus: 401 }) {}

export class NotFound extends Schema.TaggedError<NotFound>()(
  "NotFound",
  { error: Schema.String, path: Schema.optionalKey(Schema.String) },
  { httpApiStatus: 404 },
) {}

export class BadRequest extends Schema.TaggedError<BadRequest>()(
  "BadRequest",
  { error: Schema.String, issue: Schema.optionalKey(Schema.String) },
  { httpApiStatus: 400 },
) {}

/** The request can't apply in the current state: already settled, or stale. */
export class Conflict extends Schema.TaggedError<Conflict>()(
  "Conflict",
  {
    error: Schema.String,
    message: Schema.optionalKey(Schema.String),
    /** The task as it is (a cancel of a settled task). */
    record: Schema.optionalKey(TaskRecord),
    /** The human request as it is. */
    request: Schema.optionalKey(Schema.Unknown),
  },
  { httpApiStatus: 409 },
) {}

/** The workspace refused the path. */
export class Forbidden extends Schema.TaggedError<Forbidden>()("Forbidden", { error: Schema.String, message: Schema.String }, { httpApiStatus: 403 }) {}

/** An agent (or the workspace) couldn't be reached: `error` is the `AgentError` reason. Clients retry. */
export class Unavailable extends Schema.TaggedError<Unavailable>()("Unavailable", { error: Schema.String, message: Schema.String }, { httpApiStatus: 503 }) {}

/** A send refused because its task id is taken by another task or payload: the failed record (`InputConflict`), not stored. */
export class TaskRefused extends Schema.TaggedError<TaskRefused>()("TaskRefused", TaskRecord.fields, { httpApiStatus: 409 }) {}

// ---------------------------------------------------------------------------
// Authorization: `Authorization: Bearer <AGENT_API_TOKEN>` on every endpoint
// ---------------------------------------------------------------------------

export class Authorization extends HttpApiMiddleware.Service<Authorization>()("alchemy-agent/Api/Authorization", {
  security: { bearer: HttpApiSecurity.bearer },
  error: Unauthorized,
  requiredForClient: true,
}) {}

// ---------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------

export const TaskEntry = Schema.Struct({ ...TaskRecord.fields, seq: Schema.Number, input: Schema.Unknown });

export const TranscriptEntry = Schema.Struct({
  seq: Schema.Number,
  turnId: Schema.String,
  kind: Schema.String,
  message: Schema.Unknown,
  createdAt: Schema.Number,
});

/** A session's events: `message` (with the transcript `seq` as its id), `turn`, `idle`; `data` is the event's JSON. */
export const SessionEvents = HttpApiSchema.StreamSse({
  events: Schema.Struct({ event: Schema.String, id: Schema.optional(Schema.String), data: Schema.String }),
});

export const RunStatus = Schema.Struct({
  _tag: Schema.Literals(["NotStarted", "Running", "Completed", "Failed", "Interrupted", "Died"]),
  result: Schema.optionalKey(Schema.Unknown),
  error: Schema.optionalKey(Schema.Unknown),
  defect: Schema.optionalKey(Schema.String),
});

/** A human request as people see it. */
export const HumanRequestView = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  prompt: Schema.String,
  schema: Schema.Unknown,
  artifact: Schema.NullOr(Schema.Struct({ path: Schema.String, revision: Schema.String })),
  expiresAt: Schema.NullOr(Schema.Number),
  status: Schema.Literals(["pending", "resolved", "expired", "canceled", "failed"]),
  createdAt: Schema.Number,
  settledAt: Schema.NullOr(Schema.Number),
  causation: Schema.NullOr(Schema.String),
  answer: Schema.optionalKey(Schema.Unknown),
  error: Schema.optionalKey(Schema.Unknown),
});

const Accepted = Schema.Struct({}).pipe(HttpApiSchema.status(202));
const after = { after: Schema.optional(Schema.Number) };
const agent = { type: Schema.String, name: Schema.String };
/** A logical workspace path, the rest of the URL (`/workspace/files/shared/desk/a.md`). */
const path = { "*": Schema.String };

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

export class AgentsGroup extends HttpApiGroup.make("agents")
  .add(
    HttpApiEndpoint.get("registry", "/agents", {
      query: { type: Schema.optional(Schema.String) },
      success: Schema.Array(Schema.Unknown),
      error: NotFound,
    }),
    HttpApiEndpoint.post("enqueue", "/agents/:type/:name/tasks", {
      params: agent,
      payload: Schema.Struct({ tag: Schema.String, payload: Schema.optionalKey(Schema.Unknown), taskId: Schema.optionalKey(Schema.String) }),
      success: TaskRecord.pipe(HttpApiSchema.status(202)),
      error: [NotFound, TaskRefused, Unavailable],
    }),
    HttpApiEndpoint.get("tasks", "/agents/:type/:name/tasks", {
      params: agent,
      query: { status: Schema.optional(Schema.String), ...after, limit: Schema.optional(Schema.Number) },
      success: Schema.Array(TaskEntry),
      error: [NotFound, Unavailable],
    }),
    HttpApiEndpoint.get("task", "/agents/:type/:name/tasks/:taskId", {
      params: { ...agent, taskId: Schema.String },
      /** Present: hold the request until the task settles (up to 20 s). */
      query: { wait: Schema.optional(Schema.String) },
      success: TaskRecord,
      error: [NotFound, Unavailable],
    }),
    HttpApiEndpoint.post("cancel", "/agents/:type/:name/tasks/:taskId/cancel", {
      params: { ...agent, taskId: Schema.String },
      payload: Schema.Struct({ reason: Schema.optionalKey(Schema.String) }),
      success: TaskRecord,
      error: [NotFound, Conflict, Unavailable],
    }),
    HttpApiEndpoint.get("transcript", "/agents/:type/:name/sessions/:sessionId", {
      params: { ...agent, sessionId: Schema.String },
      query: after,
      success: Schema.Array(TranscriptEntry),
      error: [NotFound, Unavailable],
    }),
    HttpApiEndpoint.get("events", "/agents/:type/:name/sessions/:sessionId/events", {
      params: { ...agent, sessionId: Schema.String },
      query: after,
      success: SessionEvents,
      error: [NotFound, Unavailable],
    }),
  )
  .middleware(Authorization) {}

export class WorkspaceGroup extends HttpApiGroup.make("workspace")
  .add(
    HttpApiEndpoint.get("read", "/workspace/files/*", {
      params: path,
      success: Schema.String.pipe(HttpApiSchema.asText()),
      error: [NotFound, Forbidden, Unavailable],
    }),
    HttpApiEndpoint.put("write", "/workspace/files/*", {
      params: path,
      payload: Schema.String.pipe(HttpApiSchema.asText()),
      success: Schema.Struct({ path: Schema.String, revision: Schema.String, size: Schema.Number }),
      error: [Forbidden, Unavailable],
    }),
    HttpApiEndpoint.get("list", "/workspace/list/*", {
      params: path,
      success: Schema.Array(Schema.String),
      error: [Forbidden, Unavailable],
    }),
    HttpApiEndpoint.get("audit", "/workspace/audit", { query: after, success: Schema.Array(Schema.Unknown), error: Unavailable }),
  )
  .middleware(Authorization) {}

export class WorkflowsGroup extends HttpApiGroup.make("workflows")
  .add(
    HttpApiEndpoint.post("start", "/workflows/:name/runs", {
      params: { name: Schema.String },
      payload: Schema.Struct({ payload: Schema.optionalKey(Schema.Unknown) }),
      success: Schema.Struct({ executionId: Schema.String }).pipe(HttpApiSchema.status(202)),
      error: [NotFound, BadRequest],
    }),
    HttpApiEndpoint.get("status", "/workflows/:name/runs/:executionId", {
      params: { name: Schema.String, executionId: Schema.String },
      success: RunStatus,
      error: NotFound,
    }),
    HttpApiEndpoint.post("signal", "/workflows/:name/runs/:executionId/signals/:signal", {
      params: { name: Schema.String, executionId: Schema.String, signal: Schema.String },
      payload: Schema.Struct({ value: Schema.optionalKey(Schema.Unknown) }),
      success: Accepted,
      error: NotFound,
    }),
  )
  .middleware(Authorization) {}

export class ListenersGroup extends HttpApiGroup.make("listeners")
  .add(
    HttpApiEndpoint.post("replay", "/listeners/events", {
      payload: Schema.Struct({ message: Schema.Unknown }),
      success: Accepted,
      error: [NotFound, BadRequest],
    }),
    HttpApiEndpoint.get("log", "/listeners/log", { query: after, success: Schema.Array(Schema.Unknown), error: NotFound }),
  )
  .middleware(Authorization) {}

const Answer = Schema.Struct({ response: Schema.optionalKey(Schema.Unknown), by: Schema.optionalKey(Schema.String), revision: Schema.optionalKey(Schema.Unknown) });

export class RequestsGroup extends HttpApiGroup.make("requests")
  .add(
    HttpApiEndpoint.get("list", "/requests", {
      query: { status: Schema.optional(Schema.String), ...after },
      success: Schema.Array(HumanRequestView),
      error: [BadRequest, Unavailable],
    }),
    HttpApiEndpoint.get("get", "/requests/:id", { params: { id: Schema.String }, success: HumanRequestView, error: [NotFound, Unavailable] }),
    HttpApiEndpoint.post("respond", "/requests/:id/respond", {
      params: { id: Schema.String },
      payload: Answer,
      success: HumanRequestView,
      error: [NotFound, BadRequest, Conflict, Forbidden, Unavailable],
    }),
    HttpApiEndpoint.post("cancel", "/requests/:id/cancel", {
      params: { id: Schema.String },
      payload: Schema.Struct({ by: Schema.optionalKey(Schema.String), reason: Schema.optionalKey(Schema.String) }),
      success: HumanRequestView,
      error: [NotFound, BadRequest, Conflict, Forbidden, Unavailable],
    }),
  )
  .middleware(Authorization) {}

/** The whole API. */
export class AgentApi extends HttpApi.make("alchemy-agent")
  .add(AgentsGroup, WorkspaceGroup, WorkflowsGroup, ListenersGroup, RequestsGroup)
  .annotateMerge(OpenApi.annotations({ title: "alchemy-agent" })) {}
