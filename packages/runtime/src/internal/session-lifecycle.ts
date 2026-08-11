import type { SessionId } from "@cvr/loom-domain";
import { SessionClosingError } from "@cvr/loom-protocol";
import { Context, Effect, HashMap, Layer, Option, Ref, Semaphore, SynchronizedRef } from "effect";

interface SessionState {
  readonly admissions: Semaphore.Semaphore;
  readonly closes: Semaphore.Semaphore;
  readonly closing: Ref.Ref<boolean>;
}

const admissionPermits = 2 ** 31 - 1;
type SessionStates = SynchronizedRef.SynchronizedRef<HashMap.HashMap<SessionId, SessionState>>;

export interface SessionLifecycleShape {
  readonly isClosing: (sessionId: SessionId) => Effect.Effect<boolean>;
  readonly admit: <A, E, R>(
    sessionId: SessionId,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SessionClosingError, R>;
  readonly close: <A, E, R>(
    sessionId: SessionId,
    cleanup: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
}

export class SessionLifecycle extends Context.Service<SessionLifecycle, SessionLifecycleShape>()(
  "@cvr/loom-runtime/SessionLifecycle",
) {}

const makeSessionState = Effect.all({
  admissions: Semaphore.make(admissionPermits),
  closes: Semaphore.make(1),
  closing: Ref.make(false),
});

const stateFor = (states: SessionStates, sessionId: SessionId) =>
  SynchronizedRef.modifyEffect(states, (current) =>
    HashMap.get(current, sessionId).pipe(
      Option.match({
        onNone: () =>
          makeSessionState.pipe(
            Effect.map((state) => [state, HashMap.set(current, sessionId, state)]),
          ),
        onSome: (state) => Effect.succeed([state, current]),
      }),
    ),
  );

const isClosing = (states: SessionStates, sessionId: SessionId) =>
  SynchronizedRef.get(states).pipe(
    Effect.flatMap((current) =>
      HashMap.get(current, sessionId).pipe(
        Option.match({
          onNone: () => Effect.succeed(false),
          onSome: (state) => Ref.get(state.closing),
        }),
      ),
    ),
  );

export const makeSessionLifecycle = Effect.gen(function* () {
  const states = yield* SynchronizedRef.make(HashMap.empty<SessionId, SessionState>());

  return SessionLifecycle.of({
    isClosing: (sessionId) => isClosing(states, sessionId),
    admit: (sessionId, effect) =>
      stateFor(states, sessionId).pipe(
        Effect.flatMap((state) =>
          state.admissions.withPermits(1)(
            Ref.get(state.closing).pipe(
              Effect.filterOrFail(
                (closing) => !closing,
                () => new SessionClosingError({ sessionId }),
              ),
              Effect.andThen(effect),
            ),
          ),
        ),
      ),
    close: (sessionId, cleanup) =>
      stateFor(states, sessionId).pipe(
        Effect.flatMap((state) =>
          state.closes.withPermit(
            Ref.set(state.closing, true).pipe(
              Effect.andThen(state.admissions.withPermits(admissionPermits)(cleanup)),
            ),
          ),
        ),
      ),
  });
});

export const layerSessionLifecycle = Layer.effect(SessionLifecycle, makeSessionLifecycle);
