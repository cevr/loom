import { SessionClosureStore, type SessionClosureStoreError } from "@cvr/loom-runtime";
import { Context, Effect, Layer } from "effect";
import { makeCloseSession } from "./close-session.js";

interface SessionRecoveryShape {
  readonly recover: Effect.Effect<void, SessionClosureStoreError>;
}

export class SessionRecovery extends Context.Service<SessionRecovery, SessionRecoveryShape>()(
  "@cvr/loom-daemon/SessionRecovery",
) {}

export const layerSessionRecovery = Layer.effect(
  SessionRecovery,
  Effect.gen(function* () {
    const closures = yield* SessionClosureStore;
    const close = yield* makeCloseSession;
    return SessionRecovery.of({
      recover: closures.list.pipe(
        Effect.flatMap((sessionIds) =>
          Effect.forEach(
            sessionIds,
            (sessionId) =>
              close(sessionId).pipe(
                Effect.tapError((error) =>
                  Effect.logError("Session cleanup recovery failed.", { sessionId, error }),
                ),
                Effect.result,
              ),
            { discard: true },
          ),
        ),
      ),
    });
  }),
);
