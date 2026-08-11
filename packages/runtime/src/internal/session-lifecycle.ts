import type { SessionId } from "@cvr/loom-domain";
import { SessionClosingError } from "@cvr/loom-protocol";
import {
  Context,
  type Duration,
  Effect,
  HashMap,
  Layer,
  Option,
  Semaphore,
  SynchronizedRef,
} from "effect";
import { SessionClosureStore } from "./session-closure-store.js";
import type { SessionClosureStoreError } from "./session-closure-store-error.js";

interface SessionState {
  readonly admissions: Semaphore.Semaphore;
  readonly closes: Semaphore.Semaphore;
}

interface SessionStateEntry {
  readonly state: SessionState;
  readonly users: number;
}

const admissionPermits = 2 ** 31 - 1;
type SessionStates = SynchronizedRef.SynchronizedRef<HashMap.HashMap<SessionId, SessionStateEntry>>;

export interface SessionLifecycleConfig {
  readonly closureLease: Duration.Input;
}

export interface SessionLifecycleShape {
  readonly isClosing: (sessionId: SessionId) => Effect.Effect<boolean, SessionClosureStoreError>;
  readonly admit: <A, E, R>(
    sessionId: SessionId,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SessionClosingError | SessionClosureStoreError, R>;
  readonly close: <A, E, R>(
    sessionId: SessionId,
    cleanup: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SessionClosureStoreError, R>;
}

export class SessionLifecycle extends Context.Service<SessionLifecycle, SessionLifecycleShape>()(
  "@cvr/loom-runtime/SessionLifecycle",
) {}

const makeSessionState = Effect.all({
  admissions: Semaphore.make(admissionPermits),
  closes: Semaphore.make(1),
});

const acquireState = (states: SessionStates, sessionId: SessionId) =>
  SynchronizedRef.modifyEffect(states, (current) =>
    HashMap.get(current, sessionId).pipe(
      Option.match({
        onNone: () =>
          makeSessionState.pipe(
            Effect.map((state) => [state, HashMap.set(current, sessionId, { state, users: 1 })]),
          ),
        onSome: ({ state, users }) =>
          Effect.succeed([state, HashMap.set(current, sessionId, { state, users: users + 1 })]),
      }),
    ),
  );

const releaseState = (states: SessionStates, sessionId: SessionId, state: SessionState) =>
  SynchronizedRef.update(states, (current) =>
    HashMap.get(current, sessionId).pipe(
      Option.match({
        onNone: () => current,
        onSome: (entry) => {
          if (entry.state !== state) return current;
          if (entry.users === 1) return HashMap.remove(current, sessionId);
          return HashMap.set(current, sessionId, { state, users: entry.users - 1 });
        },
      }),
    ),
  );

const withState = <A, E, R>(
  states: SessionStates,
  sessionId: SessionId,
  use: (state: SessionState) => Effect.Effect<A, E, R>,
) =>
  Effect.acquireUseRelease(acquireState(states, sessionId), use, (state) =>
    releaseState(states, sessionId, state),
  );

const startPruner = (closures: SessionClosureStore["Service"], lease: Duration.Input) => {
  const prune = closures.prune.pipe(
    Effect.tapError((error) => Effect.logError("Session closure pruning failed.", { error })),
    Effect.ignore,
  );
  return Effect.sleep(lease).pipe(Effect.andThen(prune), Effect.forever, Effect.forkScoped);
};

export const makeSessionLifecycle = (config: SessionLifecycleConfig) =>
  Effect.gen(function* () {
    const closures = yield* SessionClosureStore;
    const states = yield* SynchronizedRef.make(HashMap.empty<SessionId, SessionStateEntry>());
    yield* startPruner(closures, config.closureLease);

    return SessionLifecycle.of({
      isClosing: closures.contains,
      admit: (sessionId, effect) =>
        withState(states, sessionId, (state) =>
          state.admissions.withPermits(1)(
            closures.contains(sessionId).pipe(
              Effect.filterOrFail(
                (closing) => !closing,
                () => new SessionClosingError({ sessionId }),
              ),
              Effect.andThen(effect),
            ),
          ),
        ),
      close: (sessionId, cleanup) =>
        withState(states, sessionId, (state) =>
          state.closes.withPermit(
            closures
              .close(sessionId, config.closureLease)
              .pipe(Effect.andThen(state.admissions.withPermits(admissionPermits)(cleanup))),
          ),
        ),
    });
  });

export const layerSessionLifecycle = (config: SessionLifecycleConfig) =>
  Layer.effect(SessionLifecycle, makeSessionLifecycle(config));
