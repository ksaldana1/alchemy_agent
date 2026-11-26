// The agents' HTTP API, and how to reach agents over it.
//
//   POST {prefix}/:type/:name/tasks             { task, input, taskId? }  → 202 TaskRecord
//   GET  {prefix}/:type/:name/tasks/:taskId     [?wait=<seconds>]        → TaskRecord | 404
//
// `?wait=1` holds the request until the task settles (up to 20 s), then returns the record.
// Every request needs `Authorization: Bearer <AGENT_API_TOKEN>`.
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Context from "effect/Context";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import {
  AgentUnreachable,
  reachKey,
  waiterKey,
  type AgentClass,
  type AgentType,
  type Reach,
  type Transport,
  type Waiter,
} from "./internal/core.ts";
import type { TaskRecord } from "./internal/inbox.ts";
import { bindingTransport, type Namespace } from "./internal/ref.ts";

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

export interface Routes {
  /** Handles requests under the prefix; anything else gets `undefined`, so you can route it. */
  readonly handle: Effect.Effect<HttpServerResponse.HttpServerResponse | undefined, never, HttpServerRequest>;
}

/**
 * The agents' HTTP API, mounted in your own Worker. In the construction phase:
 * `const api = yield* Api.routes({ agents: [Greeter] })`; in `fetch`:
 * `return (yield* api.handle) ?? (yield* yourOwnRoutes)`.
 */
export const routes = <const Agents extends ReadonlyArray<AgentClass>>(options: {
  readonly agents: Agents;
  /** Default `/agents`. */
  readonly prefix?: string;
}) =>
  Effect.gen(function* () {
    const token = yield* Config.Redacted(TOKEN_ENV);
    const prefix = (options.prefix ?? "/agents").replace(/\/$/, "");
    const byType = new Map<string, Namespace>();
    for (const agent of options.agents) {
      byType.set(agent.definition.name, (yield* (agent as unknown as Effect.Effect<Namespace>)) as Namespace);
    }

    const handle = Effect.gen(function* () {
      const request = yield* HttpServerRequest;
      const url = new URL(request.url, "http://local");
      if (!url.pathname.startsWith(`${prefix}/`)) return undefined;
      if (!timingSafeEqual(request.headers["authorization"] ?? "", `Bearer ${Redacted.value(token)}`)) {
        return yield* json({ error: "unauthorized" }, 401);
      }
      const m = url.pathname.slice(prefix.length).match(/^\/([^/]+)\/([^/]+)\/tasks(?:\/([^/]+))?$/);
      const namespace = m ? byType.get(decodeURIComponent(m[1]!)) : undefined;
      if (!m || !namespace) return yield* json({ error: "not found" }, 404);
      // The binding transport retries a failed RPC (e.g. the Durable Object reset mid-call).
      const transport = bindingTransport(namespace, decodeURIComponent(m[2]!), null);

      if (m[3] === undefined && request.method === "POST") {
        const body = (yield* request.json.pipe(Effect.orElseSucceed(() => null))) as {
          task?: unknown;
          input?: unknown;
          taskId?: unknown;
        } | null;
        if (body === null || typeof body.task !== "string") {
          return yield* json({ error: "body must be { task, input, taskId? }" }, 400);
        }
        const taskId = typeof body.taskId === "string" ? body.taskId : crypto.randomUUID();
        return yield* json(yield* transport.enqueue(taskId, body.task, body.input ?? {}), 202);
      }
      if (m[3] !== undefined && request.method === "GET") {
        const taskId = decodeURIComponent(m[3]);
        const wait = (Number(url.searchParams.get("wait") ?? 0) || 0) > 0;
        const record = yield* wait ? transport.wait(taskId) : transport.get(taskId);
        return yield* record === null ? json({ error: "not found" }, 404) : json(record);
      }
      return yield* json({ error: "method not allowed" }, 405);
    }).pipe(
      Effect.catchTag("AgentUnreachable", (e) => json({ error: e.message }, 503)),
    ) as Effect.Effect<HttpServerResponse.HttpServerResponse | undefined, never, HttpServerRequest>;

    return { handle } satisfies Routes;
  });

/**
 * From outside Cloudflare (a script, a test): a Layer that reaches the listed agents over the HTTP
 * API. `Greeter.named("a").greet({ name }).pipe(Effect.provide(Api.connect({ url, token }, Greeter)))`.
 */
export const connect = <const Agents extends ReadonlyArray<AgentClass>>(
  options: { readonly url: string; readonly token: string | Redacted.Redacted<string> },
  ...agents: Agents
): Layer.Layer<Reach<AgentType<Agents[number]>> | Waiter, never, HttpClient.HttpClient> =>
  Layer.effectContext(
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const token = typeof options.token === "string" ? options.token : Redacted.value(options.token);
      const base = options.url.replace(/\/$/, "");

      const send = (request: HttpClientRequest.HttpClientRequest) =>
        client.execute(request.pipe(HttpClientRequest.bearerToken(token))).pipe(
          Effect.flatMap((res) =>
            Effect.flatMap(res.json, (body) =>
              res.status === 404 && request.method === "GET"
                ? Effect.succeed(null)
                : res.status >= 200 && res.status < 300
                  ? Effect.succeed(body as unknown as TaskRecord)
                  : Effect.fail(
                      new AgentUnreachable({
                        message: `HTTP ${res.status}: ${String((body as { error?: unknown } | null)?.error ?? "")}`,
                      }),
                    ),
            ),
          ),
          Effect.catchTag("HttpClientError", (e) => Effect.fail(new AgentUnreachable({ message: e.message }))),
          // Every operation is idempotent, so a 5xx or a dropped connection is retried.
          Effect.retry({ schedule: Schedule.exponential("200 millis"), times: 5 }),
        );

      const transport = (type: string) => (name: string): Transport => {
        const tasks = `${base}/agents/${encodeURIComponent(type)}/${encodeURIComponent(name)}/tasks`;
        return {
          enqueue: (taskId, task, input) =>
            send(
              HttpClientRequest.post(tasks).pipe(
                HttpClientRequest.setBody(HttpBody.jsonUnsafe({ taskId, task, input })),
              ),
            ) as Effect.Effect<TaskRecord, AgentUnreachable>,
          get: (taskId) => send(HttpClientRequest.get(`${tasks}/${encodeURIComponent(taskId)}`)),
          wait: (taskId) =>
            send(HttpClientRequest.get(`${tasks}/${encodeURIComponent(taskId)}?wait=1`)),
        };
      };

      let context = Context.make(waiterKey, true as const);
      for (const agent of agents) {
        context = Context.add(context, reachKey(agent.definition.name), { transport: transport(agent.definition.name) });
      }
      return context as unknown as Context.Context<Reach<AgentType<Agents[number]>> | Waiter>;
    }),
  );

// Re-exported for Workers that build their own transports.
export { bindingTransport };
