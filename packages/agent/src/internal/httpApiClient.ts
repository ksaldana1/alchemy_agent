// The typed client of the agents' HTTP API (`HttpApiClient` from its `HttpApi` definition), shared
// by `Api.connect` (agent clients over HTTP) and `Platform.connect` (`Platform.Client`).
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import type * as HttpClientResponse from "effect/http/HttpClientResponse";
import { HttpApiClient, HttpApiMiddleware } from "effect/http-api";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import { AgentApi, Authorization } from "./httpApi.ts";
import { TaskRecord } from "./inbox.ts";

/** Set on every response the API itself sends. A 404 without it comes from the edge. */
export const API_HEADER = "x-alchemy-agent";

export interface ConnectOptions {
  readonly url: string;
  readonly token: string | Redacted.Redacted<string>;
}

/** A 404 from the edge, not from the API: a fresh workers.dev URL still converging. */
const fromEdge = (res: HttpClientResponse.HttpClientResponse) => res.status === 404 && res.headers[API_HEADER] === undefined;

/**
 * The HTTP client both clients send through: the bearer token, transient failures retried, and a
 * 404 from the edge retried until the API answers.
 */
export const transformClient = (token: string) => (client: HttpClient.HttpClient) =>
  client.pipe(
    HttpClient.mapRequest(HttpClientRequest.bearerToken(token)),
    HttpClient.transformResponse((response) =>
      response.pipe(Effect.repeat({ schedule: Schedule.exponential("500 millis"), until: (res) => !fromEdge(res), times: 8 })),
    ),
    HttpClient.retryTransient({ schedule: Schedule.exponential("200 millis"), times: 5 }),
  );

export const tokenOf = (options: ConnectOptions) => (typeof options.token === "string" ? options.token : Redacted.value(options.token));

/** The typed client. The token is attached by `transformClient`; the middleware's client side passes requests on. */
export const make = (options: ConnectOptions) =>
  HttpApiClient.make(AgentApi, {
    baseUrl: options.url.replace(/\/$/, ""),
    transformClient: transformClient(tokenOf(options)),
  }).pipe(Effect.provide(HttpApiMiddleware.layerClient(Authorization, ({ next, request }) => next(request))));

export type Client = Effect.Success<ReturnType<typeof make>>;

/** A `TaskRecord` from an error that carries one (`TaskRefused`), without the error's own fields. */
export const recordOf = (e: object): TaskRecord =>
  Object.fromEntries(Object.keys(TaskRecord.fields).flatMap((k) => (k in e ? [[k, (e as Record<string, unknown>)[k]]] : []))) as TaskRecord;

/** Raw requests through the same client, for paths the typed client can't fill (`/workspace/files/<path>`). */
export const rawRequest = (options: ConnectOptions) =>
  Effect.map(Effect.service(HttpClient.HttpClient), (http) => {
    const client = transformClient(tokenOf(options))(http);
    const base = options.url.replace(/\/$/, "");
    return (request: HttpClientRequest.HttpClientRequest) => client.execute(request.pipe(HttpClientRequest.prependUrl(base)));
  });
