// Listeners (M6): events that create work while agents are idle. Every event goes through one queue
// per platform: R2 notifications for files under a listener's prefix, cron fires (sent by the
// Worker's scheduled handler), and replays sent by hand over the API. The consumer matches each
// message to its listeners and starts a workflow run or creates an agent task. A message that keeps
// failing lands in the dead-letter queue, whose consumer logs `listener.error` and records it.
//
// Exactly once comes from the target, not the queue: a run's execution id comes from the
// workflow's `idempotencyKey`, and a task's id from the Rpc's `primaryKey` or else
// `listener/<name>/<event id>`. A duplicate delivery recreates nothing.
import * as Cloudflare from "@alchemy.run/cloudflare";
import { defaultProviderMode } from "alchemy/ProviderMode";
import type { RuntimeContext } from "alchemy/RuntimeContext";
import { resolveDurableObjectNamespace } from "alchemy/Workers/DurableObject";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PrimaryKey from "effect/PrimaryKey";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type { Rpc } from "effect/rpc";
import { type Workflow, WorkflowEngine } from "effect/workflow";
import { callAgent } from "./client.ts";
import type { Sql } from "./inbox.ts";
import { workspaceBucket } from "./workspace.ts";

// ---------------------------------------------------------------------------
// Definitions
// ---------------------------------------------------------------------------

/** A file event: a file written under a listener's prefix. */
export interface FileEvent {
  /** Logical workspace path, e.g. `shared/submissions/a.txt`. */
  readonly path: string;
  /** The file name without its directory or extension: `a`. */
  readonly basename: string;
  /** The revision written (R2's etag). */
  readonly revision: string;
  readonly size: number;
  readonly eventTime: string;
}

/** A cron fire. `scheduledTime` is the time the fire was scheduled for (stable across retries). */
export interface CronFire {
  readonly cron: string;
  readonly scheduledTime: number;
}

export type Target<E> =
  | { readonly kind: "workflow"; readonly workflow: Workflow.Any; readonly make: (event: E) => unknown }
  | {
      readonly kind: "agent";
      readonly agent: { readonly type: string; readonly protocol: ReadonlyArray<Rpc.Any> };
      readonly name: string;
      readonly tag: string;
      readonly make: (event: E) => unknown;
    };

export type Listener =
  | { readonly name: string; readonly source: { readonly kind: "files"; readonly prefix: string; readonly suffix: string | undefined }; readonly target: Target<FileEvent> }
  | { readonly name: string; readonly source: { readonly kind: "cron"; readonly cron: string }; readonly target: Target<CronFire> };

// ---------------------------------------------------------------------------
// Messages on the queue
// ---------------------------------------------------------------------------

/** What R2 puts on the queue (the fields we read). */
const R2Event = Schema.Struct({
  action: Schema.String,
  bucket: Schema.optionalKey(Schema.String),
  object: Schema.Struct({ key: Schema.String, size: Schema.optionalKey(Schema.Number), eTag: Schema.optionalKey(Schema.String) }),
  eventTime: Schema.optionalKey(Schema.String),
});
/** What the Worker's scheduled handler puts on the queue. */
const CronMessage = Schema.Struct({ kind: Schema.Literal("cron"), listener: Schema.String, cron: Schema.String, scheduledTime: Schema.Number });
const Message = Schema.Union([R2Event, CronMessage]);
type Message = typeof Message.Type;

const WRITES = new Set(["PutObject", "CompleteMultipartUpload", "CopyObject"]);

const basenameOf = (path: string) => {
  const file = path.slice(path.lastIndexOf("/") + 1);
  const dot = file.lastIndexOf(".");
  return dot > 0 ? file.slice(0, dot) : file;
};

/** The listeners a message is for, each with its event and the event's id. */
const match = (listeners: ReadonlyArray<Listener>, message: Message) => {
  const out: Array<{ readonly listener: Listener; readonly eventId: string; readonly event: FileEvent | CronFire }> = [];
  if ("kind" in message) {
    for (const l of listeners) {
      if (l.name === message.listener && l.source.kind === "cron") {
        out.push({ listener: l, eventId: String(message.scheduledTime), event: { cron: message.cron, scheduledTime: message.scheduledTime } });
      }
    }
    return out;
  }
  if (!WRITES.has(message.action)) return out;
  const path = message.object.key;
  for (const l of listeners) {
    if (l.source.kind !== "files" || !path.startsWith(l.source.prefix)) continue;
    if (l.source.suffix !== undefined && !path.endsWith(l.source.suffix)) continue;
    const revision = message.object.eTag ?? "";
    out.push({
      listener: l,
      eventId: `${path}@${revision}`,
      event: { path, basename: basenameOf(path), revision, size: message.object.size ?? 0, eventTime: message.eventTime ?? "" },
    });
  }
  return out;
};

// ---------------------------------------------------------------------------
// The record of deliveries and dead letters: one Durable Object, `AgentListeners`/`listeners`
// ---------------------------------------------------------------------------

export const LISTENERS_TYPE = "AgentListeners";

export interface LogEntry {
  readonly seq: number;
  readonly at: number;
  /** `delivered` (the target's work exists), `failed` (will be retried), or `dead-letter`. */
  readonly kind: "delivered" | "failed" | "dead-letter";
  readonly listener: string | null;
  readonly eventId: string | null;
  /** The run or task created, or the error. */
  readonly detail: string;
  readonly attempts: number;
  readonly message: unknown;
}

export type ListenersRpc = {
  record: (entry: Omit<LogEntry, "seq" | "at">) => Effect.Effect<void, never, any>;
  log: (after: number) => Effect.Effect<ReadonlyArray<LogEntry>, never, any>;
};

export const listenersObject = Effect.gen(function* () {
  const state = yield* Cloudflare.DurableObjectState;
  return Effect.gen(function* () {
    const sql = state.storage.sql as unknown as Sql;
    yield* sql.exec(`CREATE TABLE IF NOT EXISTS alchemy_listener_log (
      seq INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL, kind TEXT NOT NULL, listener TEXT, event_id TEXT,
      detail TEXT NOT NULL, attempts INTEGER NOT NULL, message TEXT NOT NULL)`);
    const rpc: ListenersRpc = {
      record: (e) =>
        sql
          .exec(
            "INSERT INTO alchemy_listener_log (at, kind, listener, event_id, detail, attempts, message) VALUES (?, ?, ?, ?, ?, ?, ?)",
            Date.now(),
            e.kind,
            e.listener,
            e.eventId,
            e.detail,
            e.attempts,
            JSON.stringify(e.message ?? null),
          )
          .pipe(Effect.asVoid),
      log: (after) =>
        Effect.flatMap(sql.exec<any>("SELECT * FROM alchemy_listener_log WHERE seq > ? ORDER BY seq LIMIT 500", after), (c) => c.toArray()).pipe(
          Effect.map((rows) =>
            rows.map((r) => ({
              seq: r.seq,
              at: r.at,
              kind: r.kind,
              listener: r.listener,
              eventId: r.event_id,
              detail: r.detail,
              attempts: r.attempts,
              message: JSON.parse(r.message),
            })),
          ),
        ),
    };
    return rpc;
  });
});

const listenersStub = Effect.flatMap(resolveDurableObjectNamespace<{ getByName: (name: string) => unknown }>(LISTENERS_TYPE), (ns) =>
  ns === undefined ? Effect.die(new Error("the listeners' log isn't hosted here")) : Effect.succeed(Cloudflare.makeRpcStub<ListenersRpc>(ns.getByName("listeners"))),
);

/** Recording is best effort: a failed write to the log never fails a delivery. */
const record = (entry: Omit<LogEntry, "seq" | "at">) =>
  Effect.flatMap(listenersStub, (stub) => stub.record(entry) as Effect.Effect<void>).pipe(Effect.catchCause(() => Effect.void));

// ---------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------

const json = (schema: Schema.Top) => Schema.toCodecJson(schema as Schema.Codec<unknown, unknown, never, never>);

/** Creates the target's work for one event. Idempotent by the target's own key. */
const deliver = (engine: Option.Option<WorkflowEngine.WorkflowEngine["Service"]>, listener: Listener, eventId: string, event: FileEvent | CronFire) =>
  Effect.gen(function* () {
    const target = listener.target as Target<FileEvent | CronFire>;
    const payload = yield* Effect.try({ try: () => target.make(event), catch: (e) => new Error(`the listener's mapping failed: ${String(e)}`) });
    if (target.kind === "workflow") {
      if (Option.isNone(engine)) return yield* Effect.fail(new Error("no workflow engine is hosted with the listeners"));
      const executionId = yield* ((target.workflow as Workflow.AnyWithProps).execute(payload as never, { discard: true }) as Effect.Effect<string, unknown, WorkflowEngine.WorkflowEngine>).pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine.value),
      );
      return `run ${target.workflow._tag}/${executionId}`;
    }
    const rpc = target.agent.protocol.find((r) => r._tag === target.tag) as Rpc.AnyWithProps | undefined;
    if (rpc === undefined) return yield* Effect.fail(new Error(`${target.agent.type} has no task ${target.tag}`));
    const schema = rpc.payloadSchema as Schema.Top & { readonly make?: (i: unknown) => unknown };
    const value = typeof schema.make === "function" ? schema.make(payload ?? {}) : payload;
    const encoded = yield* Schema.encodeUnknownEffect(json(schema))(value);
    const taskId = PrimaryKey.isPrimaryKey(value) ? `${target.tag}:${PrimaryKey.value(value)}` : `listener/${listener.name}/${eventId}`;
    const address = { type: target.agent.type, name: target.name };
    yield* callAgent(address, (stub) => stub.enqueue(taskId, target.tag, encoded, { causation: `listener/${listener.name}` }));
    return `task ${address.type}/${address.name}/${taskId}`;
  });

// ---------------------------------------------------------------------------
// Hosting
// ---------------------------------------------------------------------------

/** The queue producer and the log, for the API's replay and inspection routes. */
export const listenerRuntime: {
  current: { readonly send: (message: unknown) => Effect.Effect<void, unknown, RuntimeContext>; readonly log: (after: number) => Effect.Effect<ReadonlyArray<LogEntry>> } | undefined;
} = { current: undefined };

export interface HostOptions {
  readonly forceDestroy: boolean;
  /** Deliveries before a message is dead-lettered. Default 3. */
  readonly maxRetries?: number;
}

/**
 * The listeners, as a Layer built in the Worker's init: the queues, the R2 notification rules, the
 * cron triggers, the consumers, and the log's Durable Object.
 */
export const layer = (listeners: ReadonlyArray<Listener>, options: HostOptions) => {
  const names = new Set<string>();
  for (const l of listeners) {
    if (names.has(l.name)) throw new Error(`two listeners are named "${l.name}"; give one a name`);
    names.add(l.name);
  }
  const register = Layer.effectDiscard(
    Effect.gen(function* () {
      const queue = yield* Cloudflare.Queues.Queue("AgentListenerEvents");
      const deadLetters = yield* Cloudflare.Queues.Queue("AgentListenerDeadLetters");
      const files = listeners.filter((l) => l.source.kind === "files");
      // Deploy time only, and only live: local R2 sends no notifications (the rule would ask
      // Cloudflare about a simulated queue). Locally, put events on the queue with the API.
      const deploying = !(globalThis as { __ALCHEMY_RUNTIME__?: unknown }).__ALCHEMY_RUNTIME__;
      if (files.length > 0 && deploying && (yield* defaultProviderMode) === "live") {
        const bucket = yield* workspaceBucket(options.forceDestroy);
        // One rule per distinct key range. Cloudflare rejects overlapping ranges in one configuration.
        const rules = new Map<string, { prefix: string; suffix?: string }>();
        for (const l of files) {
          if (l.source.kind !== "files") continue;
          rules.set(`${l.source.prefix}|${l.source.suffix ?? ""}`, { prefix: l.source.prefix, ...(l.source.suffix ? { suffix: l.source.suffix } : {}) });
        }
        yield* Cloudflare.R2.BucketEventNotification("AgentListenerFiles", {
          bucketName: bucket.bucketName,
          queueId: queue.queueId,
          rules: [...rules.values()].map((r) => ({ actions: ["PutObject", "CompleteMultipartUpload", "CopyObject"], ...r })),
        });
      }
      const producer = yield* Cloudflare.Queues.WriteQueue(queue);
      const engine = yield* Effect.serviceOption(WorkflowEngine.WorkflowEngine);

      // A cron fire is put on the queue, so it gets the same retries, dead-lettering, and replays.
      for (const l of listeners) {
        if (l.source.kind !== "cron") continue;
        const { cron } = l.source;
        yield* Cloudflare.Workers.cron(cron, (controller) =>
          producer.send({ kind: "cron", listener: l.name, cron, scheduledTime: controller.scheduledTime }).pipe(Effect.orDie),
        );
      }

      yield* Cloudflare.Queues.consumeQueueMessages(
        queue,
        { batchSize: 1, maxRetries: options.maxRetries ?? 3, retryDelay: "2 seconds", deadLetterQueue: deadLetters },
        (stream) =>
          Stream.runForEach(stream, (msg) =>
            Effect.gen(function* () {
              const message = yield* Schema.decodeUnknownEffect(Message)(msg.body).pipe(
                Effect.mapError((e) => new Error(`not a listener message: ${e.message}`)),
                Effect.tapError((e) =>
                  record({ kind: "failed", listener: null, eventId: null, detail: e.message, attempts: msg.attempts, message: msg.body }),
                ),
              );
              for (const { listener, eventId, event } of match(listeners, message)) {
                const result = yield* deliver(engine, listener, eventId, event).pipe(Effect.result);
                if (result._tag === "Failure") {
                  const detail = String((result.failure as { message?: unknown })?.message ?? result.failure);
                  yield* record({ kind: "failed", listener: listener.name, eventId, detail, attempts: msg.attempts, message: msg.body });
                  return yield* Effect.fail(result.failure);
                }
                yield* Effect.logInfo("listener.delivered").pipe(Effect.annotateLogs({ listener: listener.name, eventId, target: result.success }));
                yield* record({ kind: "delivered", listener: listener.name, eventId, detail: result.success, attempts: msg.attempts, message: msg.body });
              }
            }),
          ),
      );
      // Dead letters: Cloudflare doesn't alert on them, so log and record each one.
      yield* Cloudflare.Queues.consumeQueueMessages(deadLetters, (stream) =>
        Stream.runForEach(stream, (msg) =>
          Effect.gen(function* () {
            yield* Effect.logError("listener.error").pipe(Effect.annotateLogs({ message: JSON.stringify(msg.body), attempts: msg.attempts }));
            yield* record({ kind: "dead-letter", listener: null, eventId: null, detail: "dead-lettered after its retries", attempts: msg.attempts, message: msg.body });
          }),
        ),
      );

      listenerRuntime.current = {
        send: (message) => producer.send(message),
        log: (after) => Effect.flatMap(listenersStub, (stub) => stub.log(after) as Effect.Effect<ReadonlyArray<LogEntry>>),
      };
    }),
  ).pipe(Layer.provide([Cloudflare.Queues.EventSourceLive, Cloudflare.Workers.CronEventSourceLive, Cloudflare.Queues.WriteQueueBinding]));
  const log = Cloudflare.DurableObject<{ readonly _AgentListeners: true }, ListenersRpc>()(LISTENERS_TYPE).make(listenersObject);
  return Layer.mergeAll(register, log as unknown as Layer.Layer<never, never, Cloudflare.Worker>);
};
