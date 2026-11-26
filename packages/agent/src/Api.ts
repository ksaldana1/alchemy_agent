// The agents' HTTP API, and how to reach agents over it.
//
//   POST {prefix}/:type/:name/tasks               { tag, payload, taskId? } → 202 TaskRecord
//   GET  {prefix}/:type/:name/tasks/:taskId       [?wait=1]                → TaskRecord | 404
//   GET  {prefix}/:type/:name/sessions/:id        [?after=<seq>]           → transcript
//   GET  {prefix}/:type/:name/sessions/:id/events [?after=<seq>]           → SSE: the transcript, then
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
//   GET  {prefix}/:type/:name/tasks [?status=…&after=<seq>]     → the agent's inbox (inspection)
//   GET  {prefix}  [?type=<type>]                               → the agents that have had work (eventually consistent)
//
// `?wait=1` holds the request until the task settles (up to 20 s), then returns the record.
// Every request needs `Authorization: Bearer <AGENT_API_TOKEN>`.
import * as Config from "effect/Config";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Sse from "effect/encoding/Sse";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { bindingTransport, stubFor, type AgentRpc } from "./internal/client.ts";
import { AgentError, Agents, Unreachable, type Address, type Transport } from "./internal/core.ts";
import { TaskRecord } from "./internal/inbox.ts";
import type { SessionEvent } from "./internal/session.ts";
import * as Workspace from "./Workspace.ts";
import { makeRun } from "alchemy/Workflows";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import { DurableDeferred, type Workflow, WorkflowEngine } from "effect/workflow";
import { registry } from "./internal/workflow.ts";
import { listenerRuntime } from "./internal/listeners.ts";
import { registryStub } from "./internal/registry.ts";
import * as SchemaRepresentation from "effect/SchemaRepresentation";
import { Ask, Humans, PEOPLE, RequestCanceled } from "./Human.ts";

/** The env var (bound as a Worker secret) holding the API's bearer token. */
export const TOKEN_ENV = "AGENT_API_TOKEN";

const timingSafeEqual = (a: string, b: string) => {
  const x = new TextEncoder().encode(a);
  const y = new TextEncoder().encode(b);
  let diff = x.length ^ y.length;
  for (let i = 0; i < Math.max(x.length, y.length); i++) diff |= (x[i] ?? 0) ^ (y[i] ?? 0);
  return diff === 0;
};

const json = (body: unknown, status = 200) => HttpServerResponse.json(body, { status }).pipe(Effect.orDie);

/** The body of POST …/tasks. */
const EnqueueBody = Schema.Struct({
  tag: Schema.String,
  payload: Schema.optionalKey(Schema.Unknown),
  taskId: Schema.optionalKey(Schema.String),
});
/** The API's own "not found" (as opposed to a 404 from the edge). */
const ApiNotFound = Schema.Struct({ error: Schema.Literal("not found") });

export interface Routes {
  /** Handles requests under the prefix; anything else gets `undefined`, so you can route it. */
  readonly handle: Effect.Effect<HttpServerResponse.HttpServerResponse | undefined, never, HttpServerRequest.HttpServerRequest>;
}

/**
 * The agents' HTTP API, mounted in your own Worker. In the construction phase:
 * `const api = yield* Api.routes([Greeter, Desk])`; in `fetch`:
 * `return (yield* api.handle) ?? (yield* yourOwnRoutes)`. The Worker must also host the agents
 * (provide their Layers).
 */
export const routes = (agents?: ReadonlyArray<{ readonly type: string }>, options?: { readonly prefix?: string }) =>
  Effect.gen(function* () {
    const token = yield* Config.Redacted(TOKEN_ENV);
    const prefix = (options?.prefix ?? "/agents").replace(/\/$/, "");
    // Without a list, any agent type this Worker hosts (one without a binding is "unreachable").
    const types = agents === undefined ? undefined : new Set(agents.map((a) => a.type));
    const allowed = (type: string) => types === undefined || types.has(type);
    const engine = yield* Effect.serviceOption(WorkflowEngine.WorkflowEngine);

    const handle = Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const url = new URL(request.url, "http://local");
      const isWorkspace = url.pathname.startsWith("/workspace/");
      const isWorkflow = url.pathname.startsWith("/workflows/");
      const isListeners = url.pathname.startsWith("/listeners/");
      const isRequests = url.pathname === "/requests" || url.pathname.startsWith("/requests/");
      if (url.pathname !== prefix && !url.pathname.startsWith(`${prefix}/`) && !isWorkspace && !isWorkflow && !isListeners && !isRequests) {
        return undefined;
      }
      if (!timingSafeEqual(request.headers["authorization"] ?? "", `Bearer ${Redacted.value(token)}`)) {
        return yield* json({ error: "unauthorized" }, 401);
      }
      if ((url.pathname === prefix || url.pathname === `${prefix}/`) && request.method === "GET") {
        const registry = yield* registryStub;
        if (registry === undefined) return yield* json({ error: "no agent registry is hosted here" }, 404);
        return yield* json(yield* registry.list(url.searchParams.get("type")));
      }
      if (isWorkspace) return yield* workspaceRoute(request, url);
      if (isRequests) return yield* requestsRoute(request, url);
      if (isListeners) {
        const listeners = listenerRuntime.current;
        if (listeners === undefined) return yield* json({ error: "no listeners are hosted here" }, 404);
        if (url.pathname === "/listeners/log" && request.method === "GET") {
          return yield* json(yield* listeners.log(Number(url.searchParams.get("after") ?? 0) || 0));
        }
        if (url.pathname === "/listeners/events" && request.method === "POST") {
          const body = (yield* request.json.pipe(Effect.option)).pipe(Option.getOrUndefined) as { message?: unknown } | undefined;
          if (body?.message === undefined) return yield* json({ error: "body must be { message }" }, 400);
          yield* listeners.send(body.message).pipe(Effect.orDie);
          return yield* json({}, 202);
        }
        return yield* json({ error: "not found" }, 404);
      }
      if (isWorkflow) {
        if (Option.isNone(engine)) return yield* json({ error: "no workflow engine is hosted here" }, 404);
        return yield* workflowRoute(engine.value, request, url);
      }
      const rest = url.pathname.slice(prefix.length);
      const after = Number(url.searchParams.get("after") ?? 0) || 0;

      const session = rest.match(/^\/([^/]+)\/([^/]+)\/sessions\/([^/]+)(\/events)?$/);
      if (session && request.method === "GET") {
        const address: Address = {
          type: decodeURIComponent(session[1]!),
          name: decodeURIComponent(session[2]!),
        };
        if (!allowed(address.type)) return yield* json({ error: "not found" }, 404);
        const stub = yield* stubFor(address);
        const sessionId = decodeURIComponent(session[3]!);
        if (session[4] === undefined) return yield* json(yield* stub.transcript(sessionId, after));
        const sse = (stub.events(sessionId, after) as Stream.Stream<SessionEvent>).pipe(
          Stream.map((e) =>
            Sse.encoder.write({
              _tag: "Event",
              event: e.type,
              id: e.type === "message" ? String(e.seq) : undefined,
              data: JSON.stringify(e),
            }),
          ),
          Stream.encodeText,
        );
        return HttpServerResponse.stream(sse, {
          headers: {
            "content-type": "text/event-stream",
            "cache-control": "no-cache",
          },
        });
      }

      const task = rest.match(/^\/([^/]+)\/([^/]+)\/tasks(?:\/([^/]+))?$/);
      if (!task) return yield* json({ error: "not found" }, 404);
      const address: Address = {
        type: decodeURIComponent(task[1]!),
        name: decodeURIComponent(task[2]!),
      };
      if (!allowed(address.type)) return yield* json({ error: "not found" }, 404);
      // The binding transport retries a failed RPC (e.g. the Durable Object reset mid-call).
      const transport = bindingTransport(address);

      if (task[3] === undefined && request.method === "POST") {
        const body = yield* HttpServerRequest.schemaBodyJson(EnqueueBody).pipe(Effect.option);
        if (body._tag === "None") return yield* json({ error: "body must be { tag, payload?, taskId? }" }, 400);
        const { tag, payload, taskId = crypto.randomUUID() } = body.value;
        return yield* json(yield* transport.enqueue(taskId, tag, payload ?? null), 202);
      }
      if (task[3] === undefined && request.method === "GET") {
        const stub = yield* stubFor(address);
        const statuses = url.searchParams.get("status")?.split(",").filter((x) => x !== "");
        return yield* json(yield* stub.tasks({ ...(statuses ? { statuses } : {}), after, limit: Number(url.searchParams.get("limit") ?? 100) || 100 }));
      }
      if (task[3] !== undefined && request.method === "GET") {
        const taskId = decodeURIComponent(task[3]);
        const record = yield* url.searchParams.has("wait") ? transport.wait(taskId) : transport.get(taskId);
        return yield* record === null ? json({ error: "not found" }, 404) : json(record);
      }
      return yield* json({ error: "method not allowed" }, 405);
    }).pipe(
      Effect.catchTag("AgentError", (e) => json({ error: String(e.reason._tag), message: e.message }, 503)),
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

/** Starting runs, reading them, and signalling them, for workflows registered with the engine. */
const workflowRoute = (engine: WorkflowEngine.WorkflowEngine["Service"], request: HttpServerRequest.HttpServerRequest, url: URL) =>
  Effect.gen(function* () {
    const m = url.pathname.match(/^\/workflows\/([^/]+)\/runs(?:\/([^/]+)(?:\/signals\/([^/]+))?)?$/);
    if (!m) return yield* json({ error: "not found" }, 404);
    const workflow = registry.get(decodeURIComponent(m[1]!)) as Workflow.AnyWithProps | undefined;
    if (workflow === undefined) return yield* json({ error: "not found" }, 404);
    if (m[2] === undefined && request.method === "POST") {
      const body = (yield* request.json.pipe(Effect.option)).pipe(Option.getOrUndefined) as { payload?: unknown } | undefined;
      const payload = yield* Schema.decodeUnknownEffect(codec(workflow.payloadSchema))(body?.payload ?? {}).pipe(Effect.option);
      if (payload._tag === "None") return yield* json({ error: "body must be { payload } matching the workflow's payload" }, 400);
      const executionId = yield* workflow
        .execute(payload.value as never, { discard: true })
        .pipe(Effect.provideService(WorkflowEngine.WorkflowEngine, engine)) as Effect.Effect<string>;
      return yield* json({ executionId }, 202);
    }
    if (m[2] === undefined) return yield* json({ error: "method not allowed" }, 405);
    const run = makeRun(engine, workflow as never, decodeURIComponent(m[2]));
    if (m[3] === undefined && request.method === "GET") return yield* json(yield* encodeStatus(workflow, yield* run.status));
    if (m[3] !== undefined && request.method === "POST") {
      const body = (yield* request.json.pipe(Effect.option)).pipe(Option.getOrUndefined) as { value?: unknown } | undefined;
      // Sent encoded; the run decodes it with its own signal schema.
      yield* engine.deferredDone(DurableDeferred.make(decodeURIComponent(m[3]), { success: Schema.Unknown }), {
        workflowName: workflow._tag,
        executionId: decodeURIComponent(m[2]),
        deferredName: decodeURIComponent(m[3]),
        exit: Exit.succeed(body?.value ?? null),
      });
      return yield* json({}, 202);
    }
    return yield* json({ error: "method not allowed" }, 405);
  });

// ---------------------------------------------------------------------------
// Human requests (M7): tasks of AgentHumans/people
// ---------------------------------------------------------------------------

type HumanTask = TaskRecord & { readonly seq: number; readonly input: unknown };
const AskPayload = Schema.toCodecJson(Ask.payloadSchema as Schema.Codec<unknown, unknown, never, never>);
const encodeAnswer = Schema.encodeUnknownEffect(Schema.toCodecJson(Ask.successSchema as Schema.Codec<unknown, unknown, never, never>));
const encodeAskError = Schema.encodeUnknownEffect(Schema.toCodecJson(Ask.errorSchema as Schema.Codec<unknown, unknown, never, never>));

const requestStatus = (t: TaskRecord) => {
  if (t.status === "completed") return "resolved";
  if (t.status !== "failed") return "pending";
  const tag = t.error?.kind === "declared" ? (t.error.value as { _tag?: string })?._tag : undefined;
  return tag === "RequestExpired" ? "expired" : tag === "RequestCanceled" ? "canceled" : "failed";
};

/** A request as people see it. */
const requestView = (t: HumanTask) => {
  const input = t.input as { name: string; prompt: string; schema: unknown; artifact: unknown; expiresAt: number | null };
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

const requestsRoute = (request: HttpServerRequest.HttpServerRequest, url: URL) =>
  Effect.gen(function* () {
    const people = yield* stubFor({ type: Humans.type, name: PEOPLE });
    const one = (id: string) =>
      Effect.map(people.tasks({ taskId: id, limit: 1 }) as Effect.Effect<ReadonlyArray<HumanTask>>, (ts) => ts.find((t) => t.task === "Ask"));
    if (url.pathname === "/requests" && request.method === "GET") {
      const status = url.searchParams.get("status") ?? "pending";
      if (!(status in STATUS_FILTER)) return yield* json({ error: `status must be one of ${Object.keys(STATUS_FILTER).join(", ")}` }, 400);
      const statuses = STATUS_FILTER[status];
      const tasks = (yield* people.tasks({ ...(statuses ? { statuses } : {}), after: Number(url.searchParams.get("after") ?? 0) || 0, limit: 500 })) as ReadonlyArray<HumanTask>;
      return yield* json(tasks.filter((t) => t.task === "Ask").map(requestView).filter((r) => status === "all" || r.status === status));
    }
    const m = url.pathname.match(/^\/requests\/([^/]+)(?:\/(respond|cancel))?$/);
    if (!m) return yield* json({ error: "not found" }, 404);
    const id = decodeURIComponent(m[1]!);
    const task = yield* one(id);
    if (task === undefined) return yield* json({ error: "not found" }, 404);
    if (m[2] === undefined && request.method === "GET") return yield* json(requestView(task));
    if (request.method !== "POST") return yield* json({ error: "method not allowed" }, 405);
    const settledAlready = () => json({ error: "already settled", request: requestView(task) }, 409);
    if (task.status === "completed" || task.status === "failed") return yield* settledAlready();
    const body = ((yield* request.json.pipe(Effect.option)).pipe(Option.getOrUndefined) ?? {}) as {
      response?: unknown;
      by?: unknown;
      revision?: unknown;
      reason?: unknown;
    };
    const by = typeof body.by === "string" && body.by !== "" ? body.by : undefined;
    if (by === undefined) return yield* json({ error: "body must name who answers: { by }" }, 400);

    let outcome: Parameters<AgentRpc["resolve"]>[1];
    if (m[2] === "cancel") {
      const error = yield* encodeAskError(new RequestCanceled({ request: id, by, reason: typeof body.reason === "string" ? body.reason : "" })).pipe(Effect.orDie);
      outcome = { status: "failed", error: { kind: "declared", value: error } };
    } else {
      const input = yield* Schema.decodeUnknownEffect(AskPayload)(task.input).pipe(Effect.orDie) as Effect.Effect<typeof Ask.payloadSchema.Type>;
      // The response must match the request's schema.
      const invalid = yield* Schema.decodeUnknownEffect(
        SchemaRepresentation.fromJsonSchemaDocument(input.schema as never) as Schema.Codec<unknown, unknown, never, never>,
      )(body.response).pipe(
        Effect.as(undefined),
        Effect.catch((issue) => Effect.succeed(issue.message)),
      );
      if (invalid !== undefined) return yield* json({ error: "the response doesn't match the request's schema", issue: invalid }, 400);
      // An answer about a file applies only to the revision the request is bound to, and only
      // while that is still the file's current revision.
      if (input.artifact !== null) {
        if (body.revision !== input.artifact.revision) {
          return yield* json(
            { error: "stale", message: `this request is for revision ${input.artifact.revision} of ${input.artifact.path}, not ${String(body.revision)}` },
            409,
          );
        }
        const current = yield* Workspace.platformOps.stat(input.artifact.path).pipe(
          Effect.map((ref) => ref.revision),
          Effect.catchTag("FileNotFound", () => Effect.succeed(null)),
        );
        if (current !== input.artifact.revision) {
          return yield* json(
            { error: "stale", message: `${input.artifact.path} changed since the request (now ${String(current)}); it can't be answered for ${input.artifact.revision}` },
            409,
          );
        }
      }
      const answer = yield* encodeAnswer({ response: body.response, by, at: Date.now(), revision: input.artifact?.revision ?? null }).pipe(Effect.orDie);
      outcome = { status: "completed", result: answer };
    }
    const { resolved, record } = yield* people.resolve(id, outcome);
    const after = (yield* one(id)) ?? task;
    if (!resolved) {
      // Settled meanwhile (a concurrent answer, or the expiry), or not waiting yet: nothing changes.
      return yield* json({ error: record?.status === "completed" || record?.status === "failed" ? "already settled" : "not ready yet, retry", request: requestView(after) }, 409);
    }
    return yield* json(requestView(after));
  }).pipe(
    Effect.catchTags({
      FsDenied: (e) => json({ error: "FsDenied", message: e.message }, 403),
    }),
  );

/** The platform's view of the workspace. Denials (a bad path) are 403, a missing file 404. */
const workspaceRoute = (request: HttpServerRequest.HttpServerRequest, url: URL) =>
  Effect.gen(function* () {
    const m = url.pathname.match(/^\/workspace\/(files|list|audit)(?:\/(.*))?$/);
    if (!m) return yield* json({ error: "not found" }, 404);
    const path = decodeURIComponent(m[2] ?? "");
    if (m[1] === "audit" && request.method === "GET") {
      return yield* json(yield* Workspace.platformOps.audit(Number(url.searchParams.get("after") ?? 0) || 0));
    }
    if (m[1] === "list" && request.method === "GET") return yield* json(yield* Workspace.platformOps.list(path));
    if (m[1] === "files" && request.method === "GET") {
      return HttpServerResponse.text(yield* Workspace.platformOps.read(path).pipe(Effect.map((r) => r.text)));
    }
    if (m[1] === "files" && request.method === "PUT") {
      const text = yield* request.text.pipe(Effect.orDie);
      return yield* json(yield* Workspace.platformOps.write(path, text));
    }
    return yield* json({ error: "method not allowed" }, 405);
  }).pipe(
    Effect.catchTags({
      FsDenied: (e) => json({ error: "FsDenied", message: e.message }, 403),
      FileNotFound: (e) => json({ error: "not found", path: e.path }, 404),
    }),
  );

/**
 * From outside Cloudflare (a script, a test): reach agents over the HTTP API. Every client
 * (`yield* Greeter.client`) then goes over HTTP: `program.pipe(Effect.provide(Api.connect({ url, token })))`.
 */
export const connect = (options: {
  readonly url: string;
  readonly token: string | Redacted.Redacted<string>;
}): Layer.Layer<never, never, HttpClient.HttpClient> =>
  Layer.effect(
    Agents,
    Effect.gen(function* () {
      const token = typeof options.token === "string" ? options.token : Redacted.value(options.token);
      const base = options.url.replace(/\/$/, "");
      // Transient failures (network, timeouts, 5xx) are retried; a 400 or 401 is not.
      const http = (yield* HttpClient.HttpClient).pipe(
        HttpClient.mapRequest(HttpClientRequest.bearerToken(token)),
        HttpClient.retryTransient({
          schedule: Schedule.exponential("200 millis"),
          times: 5,
        }),
      );

      const transport = (agent: Address): Transport => {
        const tasks = `${base}/agents/${encodeURIComponent(agent.type)}/${encodeURIComponent(agent.name)}/tasks`;
        const send = (request: HttpClientRequest.HttpClientRequest): Effect.Effect<TaskRecord | null, AgentError> =>
          Effect.gen(function* () {
            const res = yield* http.execute(request);
            // Only the API's own "not found" means the task doesn't exist. A 404 from the edge
            // (a fresh workers.dev URL still converging) is retried.
            if (res.status === 404 && request.method === "GET") {
              yield* HttpClientResponse.schemaBodyJson(ApiNotFound)(res).pipe(Effect.mapError(() => new EdgeNotReady()));
              return null;
            }
            return yield* HttpClientResponse.filterStatusOk(res).pipe(Effect.flatMap(HttpClientResponse.schemaBodyJson(TaskRecord)));
          }).pipe(
            Effect.retry({
              while: (e) => e._tag === "EdgeNotReady",
              schedule: Schedule.exponential("500 millis"),
              times: 8,
            }),
            Effect.mapError(
              (e) =>
                new AgentError({
                  agent,
                  reason: new Unreachable({
                    message: e._tag === "EdgeNotReady" ? "the API isn't reachable yet (404 from the edge)" : e.message,
                  }),
                }),
            ),
          );
        return {
          enqueue: (taskId, tag, payload) =>
            send(
              HttpClientRequest.post(tasks).pipe(HttpClientRequest.setBody(HttpBody.jsonUnsafe({ taskId, tag, payload }))),
            ) as Effect.Effect<TaskRecord, AgentError>,
          get: (taskId) => send(HttpClientRequest.get(`${tasks}/${encodeURIComponent(taskId)}`)),
          wait: (taskId) => send(HttpClientRequest.get(`${tasks}/${encodeURIComponent(taskId)}?wait=1`)),
        };
      };
      return { transport };
    }),
  ) as unknown as Layer.Layer<never, never, HttpClient.HttpClient>;

/** A 404 that isn't the API's own answer: the edge hasn't converged yet. */
class EdgeNotReady extends Data.TaggedError("EdgeNotReady")<{}> {}
