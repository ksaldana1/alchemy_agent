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
import { bindingTransport, stubFor } from "./internal/client.ts";
import { AgentError, Agents, Unreachable, type Address, type Transport } from "./internal/core.ts";
import { TaskRecord } from "./internal/inbox.ts";
import type { SessionEvent } from "./internal/session.ts";
import * as Workspace from "./Workspace.ts";
import { makeRun } from "alchemy/Workflows";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import { DurableDeferred, type Workflow, WorkflowEngine } from "effect/workflow";
import { registry } from "./internal/workflow.ts";

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
      if (!url.pathname.startsWith(`${prefix}/`) && !isWorkspace && !isWorkflow) return undefined;
      if (!timingSafeEqual(request.headers["authorization"] ?? "", `Bearer ${Redacted.value(token)}`)) {
        return yield* json({ error: "unauthorized" }, 401);
      }
      if (isWorkspace) return yield* workspaceRoute(request, url);
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
