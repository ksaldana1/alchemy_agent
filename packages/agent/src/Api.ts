// The agents' HTTP API: an `HttpApi` (`Api.Definition`, in `internal/httpApi.ts`) served by
// `Api.routes`, and reached from outside Cloudflare with `Api.connect`.
//
//   POST /agents/:type/:name/tasks               { tag, payload, taskId? } → 202 TaskRecord; 409 a failed
//        TaskRecord (InputConflict, not stored) when taskId is already recorded with another task or payload
//   GET  /agents/:type/:name/tasks/:taskId       [?wait=1]                → TaskRecord | 404
//   POST /agents/:type/:name/tasks/:taskId/cancel { reason? }             → 200 the canceled TaskRecord;
//        409 { error, record } already settled; 404
//   GET  /agents/:type/:name/tasks [?status=…&after=<seq>]                → the agent's inbox (inspection)
//   GET  /agents  [?type=<type>]                                          → the agents that have had work
//   GET  /agents/:type/:name/sessions/:id        [?after=<seq>]           → transcript
//   GET  /agents/:type/:name/sessions/:id/events [?after=<seq>]           → SSE: the transcript, then
//        new messages and turn changes until the session is idle; reconnect with ?after=<last seq>
//
//   GET  /workspace/files/<path>                  → the file's text | 404
//   PUT  /workspace/files/<path>   (text body)    → FileRef
//   GET  /workspace/list/<path>                   → names (directories end with /)
//   GET  /workspace/audit          [?after=<seq>] → audit entries (every allow and deny)
// The workspace routes act as the platform (full access); they need `Workspace.layer()` hosted.
//
//   POST /workflows/:name/runs                      { payload }      → 202 { executionId } (same key, same run)
//   GET  /workflows/:name/runs/:executionId                          → { _tag: NotStarted|Running|Completed|Failed|Interrupted|Died, … }
//   POST /workflows/:name/runs/:executionId/signals/:signal { value } → 202 (the first value counts)
// Workflow routes need a workflow engine in the Worker (`Platform.layer` / `Platform.make`).
//
//   POST /listeners/events  { message }    → 202: puts a message on the listeners' queue (a replay of
//                                            an R2 event, or { kind: "cron", listener, cron, scheduledTime })
//   GET  /listeners/log     [?after=<seq>] → deliveries, failures, and dead letters
//
//   GET  /requests          [?status=pending|resolved|expired|canceled|all] → human requests (M7)
//   GET  /requests/:id                                         → one request
//   POST /requests/:id/respond { response, by, revision? }     → 200 the resolved request; 400 a response
//        that doesn't match its schema; 409 stale (another revision) or already settled (a no-op)
//   POST /requests/:id/cancel  { by, reason? }                  → 200 | 409
//
// `?wait=1` holds the request until the task settles (up to 20 s), then returns the record.
// Every request needs `Authorization: Bearer <AGENT_API_TOKEN>`. Errors are JSON with an `error`
// field; an agent that can't be reached is a 503 (clients retry).
import * as Http from "alchemy/Http";
import { makeRun } from "alchemy/Workflows";
import * as Cause from "effect/Cause";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerError from "effect/http/HttpServerError";
import type * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import { HttpApiBuilder } from "effect/http-api";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as SchemaRepresentation from "effect/SchemaRepresentation";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { DurableDeferred, type Workflow, WorkflowEngine } from "effect/workflow";
import { Ask, Humans, PEOPLE, RequestCanceled } from "./Human.ts";
import { bindingTransport, isRefused, stubFor, type AgentRpc } from "./internal/client.ts";
import { AgentError, Agents, Unreachable, type Address, type Transport } from "./internal/core.ts";
import {
  AgentApi,
  Authorization,
  BadRequest,
  Conflict,
  Forbidden,
  HumanRequestView,
  NotFound,
  TaskRefused,
  Unauthorized,
  Unavailable,
} from "./internal/httpApi.ts";
import * as ApiClient from "./internal/httpApiClient.ts";
import type { TaskRecord } from "./internal/inbox.ts";
import { listenerRuntime } from "./internal/listeners.ts";
import { registryStub } from "./internal/registry.ts";
import type { SessionEvent } from "./internal/session.ts";
import { registry } from "./internal/workflow.ts";
import * as Workspace from "./Workspace.ts";

/** The API's `HttpApi` definition: for an OpenAPI document, or a client of your own. */
export { AgentApi as Definition } from "./internal/httpApi.ts";

/** The env var (bound as a Worker secret) holding the API's bearer token. */
export const TOKEN_ENV = "AGENT_API_TOKEN";

const timingSafeEqual = (a: string, b: string) => {
  const x = new TextEncoder().encode(a);
  const y = new TextEncoder().encode(b);
  let diff = x.length ^ y.length;
  for (let i = 0; i < Math.max(x.length, y.length); i++) diff |= (x[i] ?? 0) ^ (y[i] ?? 0);
  return diff === 0;
};

/** An agent that couldn't be reached is a 503 that names the reason. */
const unavailable = <A, E, R>(effect: Effect.Effect<A, E | AgentError, R>): Effect.Effect<A, Exclude<E, AgentError> | Unavailable, R> =>
  Effect.catchTag(effect as Effect.Effect<A, AgentError, R>, "AgentError", (e) =>
    Effect.fail(new Unavailable({ error: String(e.reason._tag), message: e.message })),
  ) as never;

const notFound = new NotFound({ error: "not found" });

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

export interface Routes {
  /** Handles the API's requests; anything else gets `undefined`, so you can route it. */
  readonly handle: Effect.Effect<HttpServerResponse.HttpServerResponse | undefined, never, HttpServerRequest.HttpServerRequest>;
}

/**
 * The agents' HTTP API, mounted in your own Worker. In the construction phase:
 * `const api = yield* Api.routes([Greeter, Desk])`; in `fetch`:
 * `return (yield* api.handle) ?? (yield* yourOwnRoutes)`. The Worker must also host the agents
 * (provide their Layers). Without a list, any agent type the Worker hosts is reachable.
 */
export const routes = (agents?: ReadonlyArray<{ readonly type: string }>) =>
  Effect.gen(function* () {
    const token = yield* Config.Redacted(TOKEN_ENV);
    const types = agents === undefined ? undefined : new Set(agents.map((a) => a.type));
    const engine = yield* Effect.serviceOption(WorkflowEngine.WorkflowEngine);

    const AuthorizationLive = Layer.succeed(
      Authorization,
      Authorization.of({
        bearer: (effect, { credential }) =>
          timingSafeEqual(Redacted.value(credential), Redacted.value(token)) ? effect : Effect.fail(new Unauthorized({ error: "unauthorized" })),
      }),
    );

    /** An agent this API serves, or a 404. */
    const agentOf = (params: { readonly type: string; readonly name: string }) =>
      types === undefined || types.has(params.type) ? Effect.succeed<Address>({ type: params.type, name: params.name }) : Effect.fail(notFound);

    const AgentsLive = HttpApiBuilder.group(AgentApi, "agents", (handlers) =>
      handlers
        .handle("registry", ({ query }) =>
          Effect.gen(function* () {
            const stub = yield* registryStub;
            if (stub === undefined) return yield* new NotFound({ error: "no agent registry is hosted here" });
            return (yield* stub.list(query.type ?? null)) as ReadonlyArray<unknown>;
          }),
        )
        .handle("enqueue", ({ params, payload: body }) =>
          Effect.gen(function* () {
            const address = yield* agentOf(params);
            const recorded = yield* bindingTransport(address).enqueue(body.taskId ?? crypto.randomUUID(), body.tag, body.payload ?? null);
            if (isRefused(recorded)) return yield* new TaskRefused(recorded);
            return recorded;
          }).pipe(unavailable),
        )
        .handle("tasks", ({ params, query }) =>
          Effect.gen(function* () {
            const stub = yield* stubFor(yield* agentOf(params));
            const statuses = query.status?.split(",").filter((x) => x !== "");
            return yield* stub.tasks({ ...(statuses ? { statuses } : {}), after: query.after ?? 0, limit: query.limit ?? 100 });
          }).pipe(unavailable),
        )
        .handle("task", ({ params, query }) =>
          Effect.gen(function* () {
            const transport = bindingTransport(yield* agentOf(params));
            const record = yield* query.wait !== undefined ? transport.wait(params.taskId) : transport.get(params.taskId);
            if (record === null) return yield* notFound;
            return record;
          }).pipe(unavailable),
        )
        .handle("cancel", ({ params, payload }) =>
          Effect.gen(function* () {
            const { canceled, record } = yield* bindingTransport(yield* agentOf(params)).cancel(params.taskId, payload.reason ?? "");
            if (record === null) return yield* notFound;
            if (!canceled) return yield* new Conflict({ error: "already settled", record });
            return record;
          }).pipe(unavailable),
        )
        .handle("transcript", ({ params, query }) =>
          Effect.gen(function* () {
            const stub = yield* stubFor(yield* agentOf(params));
            return yield* stub.transcript(params.sessionId, query.after ?? 0);
          }).pipe(unavailable),
        )
        .handle("events", ({ params, query }) =>
          Effect.gen(function* () {
            const stub = yield* stubFor(yield* agentOf(params));
            return (stub.events(params.sessionId, query.after ?? 0) as Stream.Stream<SessionEvent>).pipe(
              Stream.map((e) => ({ event: e.type, ...(e.type === "message" ? { id: String(e.seq) } : {}), data: JSON.stringify(e) })),
            );
          }).pipe(unavailable),
        ),
    );

    // The workspace, as the platform (full access). A denial is a 403, a missing file a 404.
    const workspaceErrors = <A, R>(effect: Effect.Effect<A, Workspace.FsDenied | Workspace.FileNotFound | AgentError, R>) =>
      effect.pipe(
        Effect.catchTags({
          FsDenied: (e) => Effect.fail(new Forbidden({ error: "FsDenied", message: e.message })),
          FileNotFound: (e) => Effect.fail(new NotFound({ error: "not found", path: e.path })),
        }),
        unavailable,
      );
    const WorkspaceLive = HttpApiBuilder.group(AgentApi, "workspace", (handlers) =>
      handlers
        .handle("read", ({ params }) => workspaceErrors(Effect.map(Workspace.platformOps.read(params["*"]), (r) => r.text)))
        .handle("write", ({ params, payload }) => workspaceErrors(Workspace.platformOps.write(params["*"], payload)) as never)
        .handle("list", ({ params }) => workspaceErrors(Workspace.platformOps.list(params["*"] ?? "")) as never)
        .handle("audit", ({ query }) => unavailable(Workspace.platformOps.audit(query.after ?? 0)) as Effect.Effect<ReadonlyArray<unknown>, Unavailable>),
    );

    const workflowOf = (name: string) =>
      Effect.gen(function* () {
        if (Option.isNone(engine)) return yield* new NotFound({ error: "no workflow engine is hosted here" });
        const workflow = registry.get(name) as Workflow.AnyWithProps | undefined;
        if (workflow === undefined) return yield* notFound;
        return { engine: engine.value, workflow };
      });
    const WorkflowsLive = HttpApiBuilder.group(AgentApi, "workflows", (handlers) =>
      handlers
        .handle("start", ({ params, payload: body }) =>
          Effect.gen(function* () {
            const { engine, workflow } = yield* workflowOf(params.name);
            const payload = yield* Schema.decodeUnknownEffect(codec(workflow.payloadSchema))(body.payload ?? {}).pipe(
              Effect.mapError(() => new BadRequest({ error: "body must be { payload } matching the workflow's payload" })),
            );
            const executionId = yield* (workflow.execute(payload as never, { discard: true }).pipe(
              Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
            ) as Effect.Effect<string>);
            return { executionId };
          }),
        )
        .handle("status", ({ params }) =>
          Effect.gen(function* () {
            const { engine, workflow } = yield* workflowOf(params.name);
            const run = makeRun(engine, workflow as never, params.executionId);
            return (yield* encodeStatus(workflow, yield* run.status)) as never;
          }),
        )
        .handle("signal", ({ params, payload }) =>
          Effect.gen(function* () {
            const { engine, workflow } = yield* workflowOf(params.name);
            // Sent encoded; the run decodes it with its own signal schema.
            yield* engine.deferredDone(DurableDeferred.make(params.signal, { success: Schema.Unknown }), {
              workflowName: workflow._tag,
              executionId: params.executionId,
              deferredName: params.signal,
              exit: Exit.succeed(payload.value ?? null),
            });
            return {};
          }),
        ),
    );

    const listenersOf = Effect.suspend(() =>
      listenerRuntime.current === undefined ? Effect.fail(new NotFound({ error: "no listeners are hosted here" })) : Effect.succeed(listenerRuntime.current),
    );
    const ListenersLive = HttpApiBuilder.group(AgentApi, "listeners", (handlers) =>
      handlers
        .handle("replay", ({ payload }) =>
          Effect.gen(function* () {
            if (payload.message === undefined) return yield* new BadRequest({ error: "body must be { message }" });
            yield* (yield* listenersOf).send(payload.message).pipe(Effect.orDie);
            return {};
          }),
        )
        .handle("log", ({ query }) => Effect.flatMap(listenersOf, (l) => l.log(query.after ?? 0)) as Effect.Effect<ReadonlyArray<unknown>, NotFound>),
    );

    const RequestsLive = HttpApiBuilder.group(AgentApi, "requests", (handlers) =>
      handlers
        .handle("list", ({ query }) => unavailable(listRequests(query.status ?? "pending", query.after ?? 0)))
        .handle("get", ({ params }) =>
          unavailable(
            Effect.gen(function* () {
              const task = yield* oneRequest(params.id);
              if (task === undefined) return yield* notFound;
              return requestView(task);
            }),
          ),
        )
        .handle("respond", ({ params, payload }) => unavailable(settleRequest(params.id, "respond", payload)))
        .handle("cancel", ({ params, payload }) => unavailable(settleRequest(params.id, "cancel", payload))),
    );

    const app = HttpApiBuilder.layer(AgentApi).pipe(
      Layer.provide([AgentsLive, WorkspaceLive, WorkflowsLive, ListenersLive, RequestsLive]),
      Layer.provide(AuthorizationLive),
      Layer.provide(Http.Platform),
    );
    // Built once for the isolate: its scope is never closed.
    const scope = yield* Scope.make();
    const serve = yield* HttpRouter.toHttpEffect(app).pipe(Scope.provide(scope));

    const handle = serve.pipe(
      Effect.catchCause((cause) => {
        const error = Cause.findErrorOption(cause);
        // Not one of the API's routes: the Worker's own routes get it.
        if (Option.isSome(error) && HttpServerError.isHttpServerError(error.value) && error.value.reason._tag === "RouteNotFound") {
          return Effect.succeed(undefined);
        }
        return Effect.map(HttpServerError.causeResponse(cause), ([response]) => response);
      }),
      Effect.map((response) => (response === undefined ? undefined : HttpServerResponse.setHeader(response, ApiClient.API_HEADER, "1"))),
      Effect.scoped,
      Effect.withSpan("Api.handle"),
    ) as Effect.Effect<HttpServerResponse.HttpServerResponse | undefined, never, HttpServerRequest.HttpServerRequest>;

    return { handle } satisfies Routes;
  });

const codec = (schema: Schema.Top) => Schema.toCodecJson(schema as Schema.Codec<unknown, unknown, never, never>);

/** A defect as text: its message, or (a tagged error without one) its JSON. */
const describeDefect = (defect: unknown): string => {
  const message = defect instanceof Error ? defect.message : "";
  if (message !== "") return message;
  try {
    return JSON.stringify(defect) ?? String(defect);
  } catch {
    return String(defect);
  }
};

/** A run's status as JSON: the result and error encoded with the workflow's schemas, a defect as text. */
export const encodeStatus = (workflow: Workflow.Any, status: Effect.Success<ReturnType<typeof makeRun>["status"]>) =>
  Effect.gen(function* () {
    const w = workflow as Workflow.AnyWithProps;
    switch (status._tag) {
      case "Completed":
        return { _tag: "Completed", result: yield* Schema.encodeUnknownEffect(codec(w.successSchema))(status.result).pipe(Effect.orDie) };
      case "Failed":
        return { _tag: "Failed", error: yield* Schema.encodeUnknownEffect(codec(w.errorSchema))(status.error).pipe(Effect.orDie) };
      case "Died":
        return { _tag: "Died", defect: describeDefect(status.defect) };
      default:
        return { _tag: status._tag };
    }
  });

// ---------------------------------------------------------------------------
// Human requests (M7): tasks of AgentHumans/people
// ---------------------------------------------------------------------------

type HumanTask = TaskRecord & { readonly seq: number; readonly input: unknown };
const AskPayload = Schema.toCodecJson(Ask.payloadSchema as Schema.Codec<unknown, unknown, never, never>);
const encodeAnswer = Schema.encodeUnknownEffect(Schema.toCodecJson(Ask.successSchema as Schema.Codec<unknown, unknown, never, never>));
const encodeAskError = Schema.encodeUnknownEffect(Schema.toCodecJson(Ask.errorSchema as Schema.Codec<unknown, unknown, never, never>));

const requestStatus = (t: TaskRecord) => {
  if (t.status === "completed") return "resolved" as const;
  if (t.status !== "failed") return "pending" as const;
  const tag = t.error?.kind === "declared" ? (t.error.value as { _tag?: string })?._tag : undefined;
  return tag === "RequestExpired" ? ("expired" as const) : tag === "RequestCanceled" ? ("canceled" as const) : ("failed" as const);
};

/** A request as people see it. */
const requestView = (t: HumanTask): typeof HumanRequestView.Type => {
  const input = t.input as { name: string; prompt: string; schema: unknown; artifact: { path: string; revision: string } | null; expiresAt: number | null };
  return {
    id: t.taskId,
    name: input.name,
    prompt: input.prompt,
    schema: input.schema,
    artifact: input.artifact,
    expiresAt: input.expiresAt,
    status: requestStatus(t),
    createdAt: t.createdAt,
    settledAt: t.settledAt,
    causation: t.causation,
    ...(t.status === "completed" ? { answer: t.result } : {}),
    ...(t.status === "failed" ? { error: t.error } : {}),
  };
};

const STATUS_FILTER: Record<string, ReadonlyArray<string> | undefined> = {
  pending: ["pending", "running", "waiting"],
  resolved: ["completed"],
  expired: ["failed"],
  canceled: ["failed"],
  all: undefined,
};

const people = stubFor({ type: Humans.type, name: PEOPLE });

const oneRequest = (id: string) =>
  Effect.flatMap(people, (stub) =>
    Effect.map(stub.tasks({ taskId: id, limit: 1 }) as Effect.Effect<ReadonlyArray<HumanTask>>, (ts) => ts.find((t) => t.task === "Ask")),
  );

const listRequests = (status: string, after: number) =>
  Effect.gen(function* () {
    if (!(status in STATUS_FILTER)) return yield* new BadRequest({ error: `status must be one of ${Object.keys(STATUS_FILTER).join(", ")}` });
    const statuses = STATUS_FILTER[status];
    const stub = yield* people;
    const tasks = (yield* stub.tasks({ ...(statuses ? { statuses } : {}), after, limit: 500 })) as ReadonlyArray<HumanTask>;
    return tasks.filter((t) => t.task === "Ask").map(requestView).filter((r) => status === "all" || r.status === status);
  });

/** A person answers or cancels a request. Only a waiting request settles; anything else is a 409. */
const settleRequest = (
  id: string,
  action: "respond" | "cancel",
  body: { readonly response?: unknown; readonly by?: string; readonly revision?: unknown; readonly reason?: string },
) =>
  Effect.gen(function* () {
    const task = yield* oneRequest(id);
    if (task === undefined) return yield* notFound;
    if (task.status === "completed" || task.status === "failed") return yield* new Conflict({ error: "already settled", request: requestView(task) });
    const by = body.by !== undefined && body.by !== "" ? body.by : undefined;
    if (by === undefined) return yield* new BadRequest({ error: "body must name who answers: { by }" });

    let outcome: Parameters<AgentRpc["resolve"]>[1];
    if (action === "cancel") {
      const error = yield* encodeAskError(new RequestCanceled({ request: id, by, reason: body.reason ?? "" })).pipe(Effect.orDie);
      outcome = { status: "failed", error: { kind: "declared", value: error } };
    } else {
      const input = (yield* Schema.decodeUnknownEffect(AskPayload)(task.input).pipe(Effect.orDie)) as typeof Ask.payloadSchema.Type;
      // The response must match the request's schema.
      const invalid = yield* Schema.decodeUnknownEffect(
        SchemaRepresentation.fromJsonSchemaDocument(input.schema as never) as Schema.Codec<unknown, unknown, never, never>,
      )(body.response).pipe(
        Effect.as(undefined),
        Effect.catch((issue) => Effect.succeed(issue.message)),
      );
      if (invalid !== undefined) return yield* new BadRequest({ error: "the response doesn't match the request's schema", issue: invalid });
      // An answer about a file applies only to the revision the request is bound to, and only
      // while that is still the file's current revision.
      if (input.artifact !== null) {
        if (body.revision !== input.artifact.revision) {
          return yield* new Conflict({
            error: "stale",
            message: `this request is for revision ${input.artifact.revision} of ${input.artifact.path}, not ${String(body.revision)}`,
          });
        }
        const current = yield* Workspace.platformOps.stat(input.artifact.path).pipe(
          Effect.map((ref) => ref.revision),
          Effect.catchTag("FileNotFound", () => Effect.succeed(null)),
          Effect.catchTag("FsDenied", (e) => Effect.fail(new Forbidden({ error: "FsDenied", message: e.message }))),
        );
        if (current !== input.artifact.revision) {
          return yield* new Conflict({
            error: "stale",
            message: `${input.artifact.path} changed since the request (now ${String(current)}); it can't be answered for ${input.artifact.revision}`,
          });
        }
      }
      const answer = yield* encodeAnswer({ response: body.response, by, at: Date.now(), revision: input.artifact?.revision ?? null }).pipe(Effect.orDie);
      outcome = { status: "completed", result: answer };
    }
    const { resolved, record } = yield* Effect.flatMap(people, (stub) => stub.resolve(id, outcome));
    const after = (yield* oneRequest(id)) ?? task;
    if (!resolved) {
      // Settled meanwhile (a concurrent answer, or the expiry), or not waiting yet: nothing changes.
      const settled = record?.status === "completed" || record?.status === "failed";
      return yield* new Conflict({ error: settled ? "already settled" : "not ready yet, retry", request: requestView(after) });
    }
    return requestView(after);
  });

// ---------------------------------------------------------------------------
// From outside Cloudflare
// ---------------------------------------------------------------------------

const messageOf = (e: unknown) =>
  typeof e === "object" && e !== null && "message" in e && typeof e.message === "string" && e.message !== "" ? e.message : String(e);

/**
 * From outside Cloudflare (a script, a test): reach agents over the HTTP API. Every client
 * (`yield* Greeter.client`) then goes over HTTP: `program.pipe(Effect.provide(Api.connect({ url, token })))`.
 */
export const connect = (options: ApiClient.ConnectOptions): Layer.Layer<never, never, HttpClient.HttpClient> =>
  Layer.effect(
    Agents,
    Effect.gen(function* () {
      const client = yield* ApiClient.make(options);
      const transport = (agent: Address): Transport => {
        const params = { type: agent.type, name: agent.name };
        const failed = (e: unknown) => new AgentError({ agent, reason: new Unreachable({ message: messageOf(e) }) });
        return {
          // A 409 is a refused send (InputConflict): its body is the failed record.
          enqueue: (taskId, tag, payload) =>
            client.agents.enqueue({ params, payload: { taskId, tag, payload } }).pipe(
              Effect.catchTag("TaskRefused", (e) => Effect.succeed(ApiClient.recordOf(e))),
              Effect.mapError(failed),
            ),
          get: (taskId) =>
            client.agents.task({ params: { ...params, taskId }, query: {} }).pipe(
              Effect.catchTag("NotFound", () => Effect.succeed(null)),
              Effect.mapError(failed),
            ),
          wait: (taskId) =>
            client.agents.task({ params: { ...params, taskId }, query: { wait: "1" } }).pipe(
              Effect.catchTag("NotFound", () => Effect.succeed(null)),
              Effect.mapError(failed),
            ),
          cancel: (taskId, reason) =>
            client.agents.cancel({ params: { ...params, taskId }, payload: { reason } }).pipe(
              Effect.map((record) => ({ canceled: true, record })),
              Effect.catchTags({
                NotFound: () => Effect.succeed({ canceled: false, record: null }),
                Conflict: (e) => Effect.succeed({ canceled: false, record: e.record ?? null }),
              }),
              Effect.mapError(failed),
            ),
        };
      };
      return { transport };
    }),
  ) as unknown as Layer.Layer<never, never, HttpClient.HttpClient>;
