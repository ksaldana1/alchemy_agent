// Signals: named, typed values sent to a waiting workflow run from outside it (M5).
//
//   export const Approve = Signal.make("approve", Schema.Struct({ approved: Schema.Boolean }))
//   // in the workflow:
//   const decision = yield* Signal.await(Approve, { timeout: "2 days" }).pipe(
//     Effect.catchTag("SignalTimeout", () => Effect.succeed({ approved: false })),
//   )
//   // from outside: POST /workflows/<Workflow>/runs/<executionId>/signals/approve  { "value": … }
//
// A signal is an `effect/workflow` `DurableDeferred`; waiting for it holds no execution. The first
// value sent is the one the run gets; later ones (or one that arrives after the timeout won) are
// ignored. The timeout is durable: it survives redeploys, and a replay doesn't restart it.
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { DurableClock, DurableDeferred, type WorkflowEngine } from "effect/workflow";

/** A signal definition: its name (unique within a workflow) and the schema of its value. */
export interface Signal<S extends Schema.Constraint> extends DurableDeferred.DurableDeferred<S> {}

export const make = <S extends Schema.Constraint>(name: string, schema: S): Signal<S> =>
  DurableDeferred.make(name, { success: schema }) as Signal<S>;

/** No value arrived before the timeout. */
export class SignalTimeout extends Schema.TaggedError<SignalTimeout>()("SignalTimeout", {
  signal: Schema.String,
  timeout: Schema.String,
}) {}

/**
 * Waits for the signal's value inside a workflow run. With `timeout`, fails with `SignalTimeout`
 * when no value arrives in time (a durable race between the signal and a durable clock).
 */
const await_ = <S extends Schema.Constraint>(
  signal: Signal<S>,
  options?: { readonly timeout?: Duration.Input | undefined },
): Effect.Effect<S["Type"], SignalTimeout, WorkflowEngine.WorkflowEngine | WorkflowEngine.WorkflowInstance | S["DecodingServices"] | S["EncodingServices"]> => {
  if (options?.timeout === undefined) return DurableDeferred.await(signal) as never;
  const timeout = Duration.fromInputUnsafe(options.timeout);
  return DurableDeferred.raceAll({
    name: `signal/${signal.name}`,
    success: signal.successSchema as unknown as Schema.Schema<S["Type"]>,
    error: SignalTimeout,
    effects: [
      DurableDeferred.await(signal),
      DurableClock.sleep({ name: `signal/${signal.name}/timeout`, duration: timeout, inMemoryThreshold: 0 }).pipe(
        Effect.andThen(Effect.fail(new SignalTimeout({ signal: signal.name, timeout: Duration.format(timeout) }))),
      ),
    ],
  }) as never;
};
export { await_ as await };
