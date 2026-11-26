// The HTTP API Worker and its typed client.
//
//   POST /agents/:type/:name/tasks          { task, input, taskId? }  → TaskRecord
//   GET  /agents/:type/:name/tasks/:taskId                             → TaskRecord | 404
//
// Every request needs `Authorization: Bearer <AGENT_API_TOKEN>`.
import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Config from "effect/Config";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Agent from "./Agent.ts";

/** The env var (bound as a Worker secret) holding the API's bearer token. */
export const TOKEN_ENV = "AGENT_API_TOKEN";

export interface WorkerOptions<Agents extends ReadonlyArray<Agent.AgentClass>> {
  /** Pass `import.meta.url` from the module that default-exports this Worker. */
  readonly main: string;
  /** The agents this API serves. Each one is hosted by this Worker. */
  readonly agents: Agents;
}

const timingSafeEqual = (a: string, b: string) => {
  const x = new TextEncoder().encode(a);
  const y = new TextEncoder().encode(b);
  let diff = x.length ^ y.length;
  for (let i = 0; i < Math.max(x.length, y.length); i++) diff |= (x[i] ?? 0) ^ (y[i] ?? 0);
  return diff === 0;
};

const json = (body: unknown, status = 200) =>
  HttpServerResponse.json(body, { status }).pipe(Effect.orDie);

/**
 * The API Worker. Default-export it from a module and yield it from your Stack:
 * `export default Api.worker("Api", { main: import.meta.url, agents: [Echo] })`.
 */
export const worker = <const Agents extends ReadonlyArray<Agent.AgentClass>>(
  name: string,
  options: WorkerOptions<Agents>,
) =>
  Cloudflare.Worker(
    name,
    { main: options.main },
    Effect.gen(function* () {
      const token = yield* Config.Redacted(TOKEN_ENV);
      const byType = new Map<string, (name: string) => Agent.Transport<never, never>>();
      for (const agent of options.agents) {
        byType.set(agent.definition.name, yield* Agent.lookupTransport(agent));
      }
      return {
        fetch: Effect.gen(function* () {
          const request = yield* HttpServerRequest;
          const auth = request.headers["authorization"] ?? "";
          if (!timingSafeEqual(auth, `Bearer ${Redacted.value(token)}`)) {
            return yield* json({ error: "unauthorized" }, 401);
          }
          const path = new URL(request.url, "http://local").pathname;
          const m = path.match(/^\/agents\/([^/]+)\/([^/]+)\/tasks(?:\/([^/]+))?$/);
          const agents = m ? byType.get(decodeURIComponent(m[1]!)) : undefined;
          if (!m || !agents) return yield* json({ error: "not found" }, 404);
          const transport = agents(decodeURIComponent(m[2]!));

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
            return yield* json(yield* transport.enqueue(taskId, body.task, body.input), 202);
          }
          if (m[3] !== undefined && request.method === "GET") {
            const record = yield* transport.get(decodeURIComponent(m[3]));
            return yield* record === null ? json({ error: "not found" }, 404) : json(record);
          }
          return yield* json({ error: "method not allowed" }, 405);
        }),
      };
    }),
  );

// ---------------------------------------------------------------------------
// Typed HTTP client
// ---------------------------------------------------------------------------

/** The API answered with something other than a task record. */
export class ApiError extends Data.TaggedError("ApiError")<{
  readonly status: number;
  readonly message: string;
}> {}

export interface ClientOptions {
  /** The API Worker's URL. */
  readonly url: string;
  readonly token: string | Redacted.Redacted<string>;
}

export interface Client {
  /** A typed client for one agent: `api.agent(Echo, "a").send("echo", { text })`. */
  agent<Ag extends Agent.AgentClass>(
    agent: Ag,
    name: string,
  ): Agent.AgentClient<Ag, ApiError, HttpClient.HttpClient>;
}

export const client = (options: ClientOptions): Client => {
  const token =
    typeof options.token === "string" ? options.token : Redacted.value(options.token);
  const base = options.url.replace(/\/$/, "");
  const send = (request: HttpClientRequest.HttpClientRequest, allowNotFound: boolean) =>
    HttpClient.execute(request.pipe(HttpClientRequest.bearerToken(token))).pipe(
      Effect.flatMap((res) =>
        Effect.flatMap(res.json, (body) =>
          res.status === 404 && allowNotFound
            ? Effect.succeed(null)
            : res.status >= 200 && res.status < 300
              ? Effect.succeed(body as unknown as Agent.TaskRecord)
              : Effect.fail(
                  new ApiError({
                    status: res.status,
                    message: String((body as { error?: unknown } | null)?.error ?? body),
                  }),
                ),
        ),
      ),
      Effect.catchTags({
        HttpClientError: (e) => Effect.fail(new ApiError({ status: 0, message: e.message })),
      }),
    );
  return {
    agent: (agent, name) => {
      const prefix = `${base}/agents/${encodeURIComponent(agent.definition.name)}/${encodeURIComponent(name)}/tasks`;
      return Agent.makeClient(agent, name, {
        enqueue: (taskId, task, input) =>
          send(
            HttpClientRequest.post(prefix).pipe(
              HttpClientRequest.setBody(HttpBody.jsonUnsafe({ taskId, task, input })),
            ),
            false,
          ) as Effect.Effect<Agent.TaskRecord, ApiError, HttpClient.HttpClient>,
        get: (taskId) =>
          send(HttpClientRequest.get(`${prefix}/${encodeURIComponent(taskId)}`), true),
      });
    },
  };
};
