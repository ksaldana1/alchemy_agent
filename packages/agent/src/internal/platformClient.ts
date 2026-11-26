// The platform from outside Cloudflare (scripts, tests): `Platform.connect({ url, token })`
// provides `Platform.Client` (files, human requests, workflow runs) and makes every agent client
// go over the HTTP API (`Api.connect`). Watching is polling: the API has no push feed yet.
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type { Workflow } from "effect/workflow";
import type { Address } from "./core.ts";
import type { TaskRecord } from "./inbox.ts";
import type { FileRef } from "./workspace.ts";

type SuccessOf<W> = W extends Workflow.Workflow<any, any, infer S, any> ? S["Type"] : never;
type ErrorOf<W> = W extends Workflow.Workflow<any, any, any, infer E> ? E["Type"] : never;
type PayloadOf<W> = W extends Workflow.Workflow<any, infer P, any, any> ? P["~type.make.in"] : never;

/** A human request as people see it, with a way to answer it. */
export interface HumanRequest {
  readonly id: string;
  readonly name: string;
  readonly prompt: string;
  readonly status: "pending" | "resolved" | "expired" | "canceled" | "failed";
  readonly artifact: { readonly path: string; readonly revision: string } | null;
  readonly causation: string | null;
  readonly expiresAt: number | null;
  /** Answers it (for an artifact, for the revision it is about). Fails with the API's error. */
  readonly respond: (response: unknown, options?: { readonly by?: string }) => Effect.Effect<HumanRequest, PlatformError>;
}

export type RunStatus<W extends Workflow.Any> =
  | { readonly _tag: "NotStarted" | "Running" | "Interrupted" }
  | { readonly _tag: "Completed"; readonly result: SuccessOf<W> }
  | { readonly _tag: "Failed"; readonly error: ErrorOf<W> }
  | { readonly _tag: "Died"; readonly defect: string };

/** One workflow run. */
export interface Run<W extends Workflow.Any> {
  readonly workflow: W;
  readonly executionId: string;
  readonly status: Effect.Effect<RunStatus<W>, PlatformError>;
  /** Waits for the run to finish: its result, or its error. A run that died is a defect. */
  readonly result: Effect.Effect<SuccessOf<W>, ErrorOf<W> | PlatformError>;
  /** The tasks this run created on an agent (its agent calls there), in order. */
  readonly tasksOn: (agent: Address) => Effect.Effect<ReadonlyArray<TaskRecord>, PlatformError>;
}

export class PlatformError extends Schema.TaggedError<PlatformError>()("PlatformError", {
  status: Schema.Number,
  message: Schema.String,
}) {}

export class Client extends Context.Service<
  Client,
  {
    readonly files: {
      readonly write: (path: string, text: string) => Effect.Effect<FileRef, PlatformError>;
      readonly read: (path: string) => Effect.Effect<string, PlatformError>;
    };
    readonly requests: {
      readonly pending: Effect.Effect<ReadonlyArray<HumanRequest>, PlatformError>;
      /** Each pending request once, as it appears. */
      readonly stream: Stream.Stream<HumanRequest, PlatformError>;
    };
    readonly listeners: {
      /** Puts a message on the listeners' queue: a replay of an R2 event, or a cron fire. */
      readonly replay: (message: unknown) => Effect.Effect<void, PlatformError>;
    };
    /** Starts a run (the same payload key is the same run). */
    readonly start: <W extends Workflow.Any>(workflow: W, payload: PayloadOf<W>) => Effect.Effect<Run<W>, PlatformError>;
    readonly run: <W extends Workflow.Any>(workflow: W, executionId: string) => Run<W>;
    /** Each run of `workflow` its listeners start, once, as it starts. */
    readonly runs: <W extends Workflow.Any>(workflow: W) => Stream.Stream<Run<W>, PlatformError>;
  }
>()("alchemy-agent/Platform/Client") {}

const json = (schema: Schema.Top) => Schema.toCodecJson(schema as Schema.Codec<unknown, unknown, never, never>);
const POLL = Schedule.spaced("1 second");

export const layer = (options: { readonly url: string; readonly token: string | Redacted.Redacted<string> }) =>
  Layer.effect(
    Client,
    Effect.gen(function* () {
      const token = typeof options.token === "string" ? options.token : Redacted.value(options.token);
      const base = options.url.replace(/\/$/, "");
      // Streams cover what happened since the client connected (allowing for clock skew).
      const connectedAt = Date.now() - 5_000;
      const http = (yield* HttpClient.HttpClient).pipe(HttpClient.mapRequest(HttpClientRequest.bearerToken(token)));

      const call = (request: HttpClientRequest.HttpClientRequest): Effect.Effect<any, PlatformError> =>
        http.execute(request.pipe(HttpClientRequest.prependUrl(base))).pipe(
          Effect.flatMap((res) =>
            Effect.flatMap(res.text, (text) => {
              const body = text === "" ? null : (() => { try { return JSON.parse(text); } catch { return text; } })();
              if (res.status < 400) return Effect.succeed(body);
              const message = typeof body === "string" ? body.slice(0, 200) : (body?.message ?? body?.error ?? res.status.toString());
              // A 404 that isn't the API's own JSON comes from the edge: a fresh workers.dev URL
              // still converging. Status 0 marks it for a retry.
              return Effect.fail(new PlatformError({ status: res.status === 404 && typeof body === "string" ? 0 : res.status, message }));
            }),
          ),
          Effect.catchTag("HttpClientError", (e) => Effect.fail(new PlatformError({ status: 0, message: e.message }))),
          // Network failures and the edge not being ready are retried; the API's own errors aren't.
          Effect.retry({ while: (e) => e.status === 0 || e.status >= 502, schedule: Schedule.exponential("500 millis"), times: 8 }),
        );
      const get = (path: string) => call(HttpClientRequest.get(path));
      const post = (path: string, body: unknown) => call(HttpClientRequest.post(path).pipe(HttpClientRequest.setBody(HttpBody.jsonUnsafe(body))));

      const request = (r: any): HumanRequest => ({
        ...r,
        respond: (response, opts) =>
          Effect.map(
            post(`/requests/${encodeURIComponent(r.id)}/respond`, { response, by: opts?.by ?? "platform-client", revision: r.artifact?.revision }),
            request,
          ),
      });

      const run = <W extends Workflow.Any>(workflow: W, executionId: string): Run<W> => {
        const w = workflow as unknown as Workflow.AnyWithProps;
        const status = Effect.flatMap(get(`/workflows/${w._tag}/runs/${executionId}`), (s: any) => {
          if (s._tag === "Completed") return Effect.map(Schema.decodeUnknownEffect(json(w.successSchema))(s.result).pipe(Effect.orDie), (result) => ({ _tag: "Completed", result }));
          if (s._tag === "Failed") return Effect.map(Schema.decodeUnknownEffect(json(w.errorSchema))(s.error).pipe(Effect.orDie), (error) => ({ _tag: "Failed", error }));
          return Effect.succeed(s);
        }) as Effect.Effect<RunStatus<W>, PlatformError>;
        return {
          workflow,
          executionId,
          status,
          result: status.pipe(
            Effect.repeat({ schedule: POLL, while: (s) => s._tag === "Running" || s._tag === "NotStarted" }),
            Effect.flatMap((s) =>
              s._tag === "Completed"
                ? Effect.succeed(s.result)
                : s._tag === "Failed"
                  ? Effect.fail(s.error)
                  : Effect.die(new Error(`run ${executionId}: ${s._tag}${"defect" in s ? `: ${s.defect}` : ""}`)),
            ),
          ) as never,
          tasksOn: (agent) =>
            Effect.map(get(`/agents/${encodeURIComponent(agent.type)}/${encodeURIComponent(agent.name)}/tasks?limit=500`), (ts: ReadonlyArray<TaskRecord>) =>
              ts.filter((t) => t.causation === `workflow/${w._tag}/${executionId}`),
            ),
        };
      };

      /** The listener log after `after`, all pages. */
      const logAfter = (after: number) =>
        Effect.gen(function* () {
          const out: Array<{ seq: number; at: number; kind: string; detail: string }> = [];
          let cursor = after;
          for (;;) {
            const page = (yield* get(`/listeners/log?after=${cursor}`)) as ReadonlyArray<{ seq: number; at: number; kind: string; detail: string }>;
            out.push(...page);
            if (page.length < 500) return out;
            cursor = page.at(-1)!.seq;
          }
        });

      /** Polls `source`, emitting each item whose key wasn't seen before. */
      const watch = <A>(source: Effect.Effect<ReadonlyArray<A>, PlatformError>, key: (a: A) => string) => {
        const seen = new Set<string>();
        return Stream.repeat(Stream.fromEffect(source), POLL).pipe(
          Stream.flatMap((items) => Stream.fromIterable(items.filter((a) => (seen.has(key(a)) ? false : (seen.add(key(a)), true))))),
        );
      };

      return Client.of({
        files: {
          write: (path, text) => call(HttpClientRequest.put(`/workspace/files/${path}`).pipe(HttpClientRequest.setBody(HttpBody.text(text)))),
          read: (path) => Effect.map(get(`/workspace/files/${path}`), String),
        },
        requests: {
          pending: Effect.map(get(`/requests?status=pending`), (rs: ReadonlyArray<any>) => rs.map(request)),
          // Requests made since this client connected.
          stream: watch(
            Effect.map(get(`/requests?status=pending`), (rs: ReadonlyArray<any>) => rs.filter((r) => r.createdAt >= connectedAt).map(request)),
            (r) => r.id,
          ),
        },
        listeners: {
          replay: (message) => Effect.asVoid(post(`/listeners/events`, { message })),
        },
        start: (workflow, payload) =>
          Effect.gen(function* () {
            const w = workflow as unknown as Workflow.AnyWithProps;
            const encoded = yield* Schema.encodeUnknownEffect(json(w.payloadSchema))(payload).pipe(Effect.orDie);
            const { executionId } = yield* post(`/workflows/${w._tag}/runs`, { payload: encoded });
            return run(workflow, executionId);
          }),
        run,
        runs: (workflow) => {
          const prefix = `run ${(workflow as unknown as Workflow.AnyWithProps)._tag}/`;
          // Runs started since this client connected: follow the log, newest entries only.
          return Stream.suspend(() => {
            let cursor = 0;
            const next = Effect.suspend(() =>
              Effect.map(logAfter(cursor), (entries) => {
                cursor = entries.at(-1)?.seq ?? cursor;
                return entries
                  .filter((e) => e.kind === "delivered" && e.at >= connectedAt && e.detail.startsWith(prefix))
                  .map((e) => e.detail.slice(prefix.length));
              }),
            );
            return watch(next, (id) => id).pipe(Stream.map((id) => run(workflow, id)));
          });
        },
      });
    }),
  );
