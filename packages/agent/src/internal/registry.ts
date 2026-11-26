// The agent registry (M7, inspection): which agents exist. An eventually consistent projection:
// each agent reports itself (best effort) the first time it receives a task in an activation, so
// `GET /agents` lists every agent that has ever had work, with when it was first and last seen.
import * as Cloudflare from "@alchemy.run/cloudflare";
import { resolveDurableObjectNamespace } from "alchemy/Workers/DurableObject";
import * as Effect from "effect/Effect";
import type { Address } from "./core.ts";
import type { Sql } from "./inbox.ts";

export const REGISTRY_TYPE = "AgentRegistry";

export interface AgentEntry {
  readonly type: string;
  readonly name: string;
  readonly firstSeen: number;
  readonly lastSeen: number;
}

export type RegistryRpc = {
  seen: (agent: Address, at: number) => Effect.Effect<void, never, any>;
  list: (type: string | null) => Effect.Effect<ReadonlyArray<AgentEntry>, never, any>;
};

export const registryObject = Effect.gen(function* () {
  const state = yield* Cloudflare.DurableObjectState;
  return Effect.gen(function* () {
    const sql = state.storage.sql as unknown as Sql;
    yield* sql.exec(`CREATE TABLE IF NOT EXISTS alchemy_agent_registry (
      type TEXT NOT NULL, name TEXT NOT NULL, first_seen INTEGER NOT NULL, last_seen INTEGER NOT NULL, PRIMARY KEY (type, name))`);
    const rpc: RegistryRpc = {
      seen: (agent, at) =>
        sql
          .exec(
            `INSERT INTO alchemy_agent_registry (type, name, first_seen, last_seen) VALUES (?, ?, ?, ?)
             ON CONFLICT (type, name) DO UPDATE SET last_seen = MAX(last_seen, excluded.last_seen)`,
            agent.type,
            agent.name,
            at,
            at,
          )
          .pipe(Effect.asVoid),
      list: (type) =>
        Effect.flatMap(
          type === null
            ? sql.exec<any>("SELECT * FROM alchemy_agent_registry ORDER BY type, name")
            : sql.exec<any>("SELECT * FROM alchemy_agent_registry WHERE type = ? ORDER BY name", type),
          (c) => c.toArray(),
        ).pipe(Effect.map((rows) => rows.map((r) => ({ type: r.type, name: r.name, firstSeen: r.first_seen, lastSeen: r.last_seen })))),
    };
    return rpc;
  });
});

export const registryStub = Effect.flatMap(resolveDurableObjectNamespace<{ getByName: (name: string) => unknown }>(REGISTRY_TYPE), (ns) =>
  Effect.succeed(ns === undefined ? undefined : Cloudflare.makeRpcStub<RegistryRpc>(ns.getByName("registry"))),
);

/** Reports an agent as seen. Best effort: never fails, and does nothing without a registry. */
export const reportSeen = (agent: Address, at: number) =>
  Effect.flatMap(registryStub, (stub) => (stub === undefined ? Effect.void : (stub.seen(agent, at) as Effect.Effect<void>))).pipe(
    Effect.catchCause(() => Effect.void),
  );

export const layer = Cloudflare.DurableObject<{ readonly _AgentRegistry: true }, RegistryRpc>()(REGISTRY_TYPE).make(registryObject);
