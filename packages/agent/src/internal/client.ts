// Clients: `(yield* Greeter.client)("a").Greet(payload)`. A client is built from the agent's Rpc
// protocol; how it reaches the agent comes from the `Agents` reference (bindings inside
// Cloudflare, HTTP outside).
import * as Cloudflare from "@alchemy.run/cloudflare";
import { resolveDurableObjectNamespace } from "alchemy/Workers/DurableObject";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as PrimaryKey from "effect/PrimaryKey";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import type * as Stream from "effect/Stream";
import type { Rpc } from "effect/rpc";
import {
  AgentContext,
  AgentError,
  AgentErrorReason,
  bindingTransportRef,
  Calls,
  RetriesExhausted,
  Unreachable,
  type Address,
  type Transport,
} from "./core.ts";
import type { StoredFailure, TaskRecord } from "./inbox.ts";
import type { SessionEvent } from "./session.ts";

export interface AgentContract {
  readonly type: string;
  readonly rpcs: ReadonlyMap<string, Rpc.AnyWithProps>;
}

export interface CallOptions<Discard extends boolean = false> {
  /** Send without waiting for the answer. */
  readonly discard?: Discard;
}

/** A task with no payload takes none. */
type PayloadArg<R> = [Rpc.PayloadConstructor<R>] extends [void] ? [payload?: void] : [payload: Rpc.PayloadConstructor<R>];

/** One agent, by name. Each task is a method, as with an Effect cluster entity client. */
export type AgentClient<Rpcs extends Rpc.Any> = {
  readonly [R in Rpcs as Rpc.Tag<R>]: {
    (...args: [...PayloadArg<R>, options?: CallOptions<false>]): Effect.Effect<Rpc.Success<R>, Rpc.Error<R> | AgentError>;
    (...args: [...PayloadArg<R>, options: CallOptions<true>]): Effect.Effect<void, AgentError>;
  };
};

const json = (schema: Schema.Top) => Schema.toCodecJson(schema as Schema.Codec<unknown, unknown, never, never>);

/** The task id a payload's `primaryKey` gives, if it has one. */
const keyedTaskId = (tag: string, payload: unknown) =>
  PrimaryKey.isPrimaryKey(payload) ? `${tag}:${PrimaryKey.value(payload)}` : undefined;

const isSettled = (r: TaskRecord) => r.status === "completed" || r.status === "failed";

/** The outcome of a settled record: the decoded success, the declared error, or an AgentError. */
export const outcome = Effect.fnUntraced(function* (agent: Address, rpc: Rpc.AnyWithProps, record: TaskRecord) {
  if (record.status === "completed") {
    return yield* Schema.decodeUnknownEffect(json(rpc.successSchema))(record.result).pipe(Effect.orDie);
  }
  const failure = record.error as StoredFailure | undefined;
  if (failure?.kind === "declared") {
    return yield* Effect.flip(Schema.decodeUnknownEffect(json(rpc.errorSchema))(failure.value).pipe(Effect.orDie));
  }
  const reason: typeof AgentErrorReason.Type =
    failure?.kind === "agent"
      ? ((yield* Schema.decodeUnknownEffect(json(AgentErrorReason))(failure.reason).pipe(Effect.orDie)) as typeof AgentErrorReason.Type)
      : new RetriesExhausted({
          attempts: record.attempts,
          lastError: "failed without a recorded error",
        });
  return yield* new AgentError({ agent, reason });
});

/** Waits for a task to settle, by asking the transport to wait (it returns periodically). */
const awaitSettled = (transport: Transport, agent: Address, taskId: string) =>
  transport.wait(taskId).pipe(
    Effect.repeat({ until: (r) => r === null || isSettled(r) }),
    Effect.flatMap((r) =>
      r === null
        ? Effect.fail(
            new AgentError({
              agent,
              reason: new Unreachable({ message: `no task ${taskId}` }),
            }),
          )
        : Effect.succeed(r),
    ),
  );

export const makeClient = <Rpcs extends Rpc.Any>(contract: AgentContract, agent: Address, transport: Transport): AgentClient<Rpcs> => {
  const methods: Record<string, unknown> = {};
  for (const [tag, rpc] of contract.rpcs) {
    // One span per call, named like the RpcClient's: `Greeter.Greet`.
    methods[tag] = Effect.fn(`${agent.type}.${tag}`)(function* (...args: ReadonlyArray<unknown>) {
      const last = args.at(-1);
      const options = Predicate.isObject(last) && "discard" in last ? (last as CallOptions<boolean>) : undefined;
      const input = options === undefined ? args[0] : args.length > 1 ? args[0] : undefined;
      const schema = rpc.payloadSchema as Schema.Top & {
        readonly make?: (i: unknown) => unknown;
      };
      const payload = typeof schema.make === "function" ? schema.make(input ?? {}) : input;
      const encoded = yield* Schema.encodeUnknownEffect(json(schema))(payload).pipe(
        Effect.mapError(
          (e) =>
            new AgentError({
              agent,
              reason: new Unreachable({
                message: `invalid payload: ${e.message}`,
              }),
            }),
        ),
      );
      const wait = options?.discard !== true;
      // Inside an agent, the call goes through its outbox (recorded with the execution's outcome).
      const calls = yield* Effect.serviceOption(Calls);
      if (calls._tag === "Some") {
        const settled = yield* calls.value.call({ agent, tag, taskId: keyedTaskId(tag, payload), payload: encoded, wait });
        return settled === undefined ? undefined : yield* outcome(agent, rpc, settled);
      }
      const recorded = yield* transport.enqueue(keyedTaskId(tag, payload) ?? crypto.randomUUID(), tag, encoded);
      if (!wait) return undefined;
      return yield* outcome(agent, rpc, yield* awaitSettled(transport, agent, recorded.taskId));
    });
  }
  return methods as AgentClient<Rpcs>;
};

// ---------------------------------------------------------------------------
// The transport over Durable Object bindings
// ---------------------------------------------------------------------------

/**
 * The agent Durable Object's RPC surface. Framework-internal. A `type`, not an `interface`:
 * Alchemy's `MainRpc` constraint is an index signature, which interfaces don't satisfy. Only add
 * methods: after a deploy, an instance can run the previous code for a few minutes.
 */
export type AgentRpc = {
  enqueue: (
    taskId: string,
    tag: string,
    payload: unknown,
    options: { readonly causation: string | null },
  ) => Effect.Effect<TaskRecord, never, any>;
  get: (taskId: string) => Effect.Effect<TaskRecord | null, never, any>;
  wait: (taskId: string, timeoutMs: number) => Effect.Effect<TaskRecord | null, never, any>;
  transcript: (sessionId: string, after: number) => Effect.Effect<ReadonlyArray<TranscriptEntry>, never, any>;
  events: (sessionId: string, after: number) => Stream.Stream<SessionEvent, never, any>;
  /**
   * M3: `replyTo` (an agent and its outbox row) is told when the task settles. Returns the
   * current record (already settled: no reply follows), or null for an unknown task.
   */
  subscribe: (taskId: string, replyTo: { readonly type: string; readonly name: string; readonly id: string }) => Effect.Effect<TaskRecord | null, never, any>;
  /** M3: the answer to a call this agent waits for (its outbox row `id`). Idempotent. */
  reply: (id: string, record: TaskRecord) => Effect.Effect<void, never, any>;
};

export interface TranscriptEntry {
  readonly seq: number;
  readonly turnId: string;
  readonly kind: string;
  readonly message: unknown;
  readonly createdAt: number;
}

/** How long one `wait` RPC holds before returning the current record. */
export const WAIT_CHUNK_MS = 20_000;

/** A stub for agent `address`, resolved from the Worker's environment at call time. */
export const stubFor = (address: Address) =>
  Effect.flatMap(resolveDurableObjectNamespace<{ getByName: (name: string) => unknown }>(address.type), (ns) =>
    ns === undefined
      ? Effect.fail(
          new AgentError({
            agent: address,
            reason: new Unreachable({
              message: `no binding for agent type "${address.type}"`,
            }),
          }),
        )
      : Effect.succeed(Cloudflare.makeRpcStub<AgentRpc>(ns.getByName(address.name))),
  );

/**
 * A failed RPC (e.g. the Durable Object reset mid-call) becomes an AgentError after a few
 * retries. Every operation is idempotent, so retrying is safe.
 */
export const callAgent = <A>(address: Address, call: (stub: AgentRpc) => Effect.Effect<A, never, any>) =>
  Effect.flatMap(stubFor(address), (stub) => call(stub) as unknown as Effect.Effect<A, unknown>).pipe(
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.failCause(cause as Cause.Cause<never>)
        : Effect.fail(
            Cause.squash(cause) instanceof AgentError
              ? (Cause.squash(cause) as AgentError)
              : new AgentError({
                  agent: address,
                  reason: new Unreachable({
                    message: String(Cause.squash(cause)),
                  }),
                }),
          ),
    ),
  );

const reliably = <A>(address: Address, call: (stub: AgentRpc) => Effect.Effect<A, never, any>) =>
  callAgent(address, call).pipe(Effect.retry({ schedule: Schedule.exponential("200 millis"), times: 6 }));

export const bindingTransport = (address: Address): Transport => {
  // The current task, if any, is the new task's causation.
  const causation = Effect.map(Effect.serviceOption(AgentContext), (ctx) =>
    ctx._tag === "Some" ? `${ctx.value.agent.type}/${ctx.value.agent.name}/${ctx.value.taskId}` : null,
  );
  return {
    enqueue: (taskId, tag, payload) =>
      Effect.flatMap(causation, (c) => reliably(address, (stub) => stub.enqueue(taskId, tag, payload, { causation: c }))),
    get: (taskId) => reliably(address, (stub) => stub.get(taskId)),
    // If `wait` fails (right after a deploy an instance can still run older code), fall back to
    // reading the record after a pause: waiting is never worse than polling.
    wait: (taskId) =>
      reliably(address, (stub) => stub.wait(taskId, WAIT_CHUNK_MS)).pipe(
        Effect.catch(() => Effect.sleep("1 second").pipe(Effect.andThen(reliably(address, (stub) => stub.get(taskId))))),
      ),
  };
};
bindingTransportRef.current = bindingTransport;
