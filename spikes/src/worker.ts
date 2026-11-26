import * as Cloudflare from "@alchemy.run/cloudflare";
import * as Effect from "effect/Effect";
import { HttpServerRequest } from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Agent from "./agent.ts";
import Counter from "./counter.ts";
import LlmProbe from "./llm.ts";
import Probe from "./probe.ts";
import * as Stream from "effect/Stream";
import Echo from "./echo.ts";
import Relay from "./relay.ts";

// Spike Worker. Each spike owns one route prefix: 0.1 /counter, 0.2 /echo and /relay,
// 0.3 /probe, 0.5 /llm.
export default Cloudflare.Worker(
  "SpikesApi",
  { main: import.meta.url },
  Effect.gen(function* () {
    const counters = yield* Counter;
    const echoes = yield* Echo;
    const relays = yield* Relay;
    const probes = yield* Probe;
    const llms = yield* LlmProbe;
    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const path = new URL(request.url, "http://local").pathname;

        const counter = path.match(/^\/counter\/([^/]+)\/(increment|get)$/);
        if (counter) {
          const name = decodeURIComponent(counter[1]!);
          const stub = counters.getByName(name);
          const count = counter[2] === "increment" ? yield* stub.increment() : yield* stub.get();
          return yield* HttpServerResponse.json({ name, count, v: 2 });
        }

        // POST /echo/:name/tasks { taskId, task, input }: approach A, the typed stub directly.
        const echoPost = path.match(/^\/echo\/([^/]+)\/tasks$/);
        if (echoPost && request.method === "POST") {
          const body = (yield* request.json) as { taskId: string; task: string; input: unknown };
          const stub = echoes.getByName(decodeURIComponent(echoPost[1]!));
          // The stub's `enqueue` is generic over task names; an HTTP body is untyped, so cast.
          const handle = yield* stub.enqueue(body.taskId, body.task as "echo", body.input as never);
          return yield* HttpServerResponse.json(handle);
        }

        // POST /relay/:name/tasks { taskId, task, input }: approach C, the definition-typed client.
        const relayPost = path.match(/^\/relay\/([^/]+)\/tasks$/);
        if (relayPost && request.method === "POST") {
          const body = (yield* request.json) as { taskId: string; task: string; input: unknown };
          const relay = Agent.client(Relay, relays.getByName(decodeURIComponent(relayPost[1]!)));
          const handle = yield* relay.send(body.taskId, body.task as "relay", body.input as never);
          return yield* HttpServerResponse.json(handle);
        }

        // L7: GET /llm/:name/stream?text=…  → server-sent events, one per text delta.
        const llmStream = path.match(/^\/llm\/([^/]+)\/stream$/);
        if (llmStream) {
          const url = new URL(request.url, "http://local");
          const deltas = llms
            .getByName(decodeURIComponent(llmStream[1]!))
            .stream(url.searchParams.get("text") ?? "Count from 1 to 10.", url.searchParams.get("model") ?? "");
          const sse = (deltas as unknown as Stream.Stream<string>).pipe(
            Stream.map((d) => `data: ${JSON.stringify(d)}\n\n`),
            Stream.encodeText,
          );
          return HttpServerResponse.stream(sse, { headers: { "content-type": "text/event-stream" } });
        }

        // /llm/:name/:method, like /probe (0.5).
        const llm = path.match(/^\/llm\/([^/]+)\/([a-zA-Z]+)$/);
        if (llm) {
          const stub = llms.getByName(decodeURIComponent(llm[1]!)) as unknown as Record<
            string,
            (...args: ReadonlyArray<unknown>) => Effect.Effect<unknown>
          >;
          const args = request.method === "POST" ? ((yield* request.json) as ReadonlyArray<unknown>) : [];
          return yield* HttpServerResponse.json(yield* stub[llm[2]!]!(...args));
        }

        // /probe/:name/:method. GET calls it with no arguments; POST takes a JSON array of them.
        const probe = path.match(/^\/probe\/([^/]+)\/([a-zA-Z]+)$/);
        if (probe) {
          const stub = probes.getByName(decodeURIComponent(probe[1]!)) as unknown as Record<
            string,
            (...args: ReadonlyArray<unknown>) => Effect.Effect<unknown>
          >;
          const method = stub[probe[2]!];
          if (typeof method !== "function") {
            return HttpServerResponse.text("No such probe method", { status: 404 });
          }
          const args =
            request.method === "POST" ? ((yield* request.json) as ReadonlyArray<unknown>) : [];
          return yield* HttpServerResponse.json(yield* method(...args));
        }

        // GET /(echo|relay)/:name/tasks/:taskId
        const get = path.match(/^\/(echo|relay)\/([^/]+)\/tasks\/([^/]+)$/);
        if (get) {
          const name = decodeURIComponent(get[2]!);
          const taskId = decodeURIComponent(get[3]!);
          const record =
            get[1] === "echo"
              ? yield* echoes.getByName(name).result(taskId)
              : yield* relays.getByName(name).result(taskId);
          return yield* HttpServerResponse.json(record ?? null, { status: record ? 200 : 404 });
        }

        return HttpServerResponse.text("Not found", { status: 404 });
      }).pipe(
        // Task errors arrive over RPC as plain `{ _tag, ...fields }` objects; catchTag matches
        // on `_tag`, so it still works without the class.
        Effect.catchTags({
          TaskInputError: (e) => HttpServerResponse.json(e, { status: 400 }),
          UnknownTaskError: (e) => HttpServerResponse.json(e, { status: 400 }),
        }),
      ),
    };
  }),
);
