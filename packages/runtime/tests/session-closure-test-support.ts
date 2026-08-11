import type { SessionId } from "@cvr/loom-domain";
import {
  makeSessionLifecycle,
  SessionClosureStore,
  type SessionClosureStoreShape,
} from "../src/index.js";
import { Clock, Duration, Effect, HashMap, Option, Ref } from "effect";

export const makeTestSessionClosureStore = Effect.gen(function* () {
  const closures = yield* Ref.make(HashMap.empty<SessionId, number>());

  return SessionClosureStore.of({
    close: (sessionId, lease) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          Ref.update(closures, (current) =>
            HashMap.get(current, sessionId).pipe(
              Option.match({
                onNone: () => HashMap.set(current, sessionId, now + Duration.toMillis(lease)),
                onSome: (retainUntil) => {
                  if (retainUntil > now) return current;
                  return HashMap.set(current, sessionId, now + Duration.toMillis(lease));
                },
              }),
            ),
          ),
        ),
      ),
    contains: (sessionId) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          Ref.get(closures).pipe(
            Effect.map((current) =>
              HashMap.get(current, sessionId).pipe(
                Option.exists((retainUntil) => retainUntil > now),
              ),
            ),
          ),
        ),
      ),
    list: Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) =>
        Ref.get(closures).pipe(Effect.map(HashMap.filter((retainUntil) => retainUntil > now))),
      ),
      Effect.map((current) => Array.from(HashMap.keys(current))),
    ),
    prune: Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) =>
        Ref.modify(closures, (current) => {
          const retained = HashMap.filter(current, (retainUntil) => retainUntil > now);
          return [HashMap.size(current) - HashMap.size(retained), retained];
        }),
      ),
    ),
  } satisfies SessionClosureStoreShape);
});

export const makeTestSessionLifecycle = makeTestSessionClosureStore.pipe(
  Effect.flatMap((store) =>
    makeSessionLifecycle({ closureLease: "5 minutes" }).pipe(
      Effect.provideService(SessionClosureStore, store),
      Effect.scoped,
    ),
  ),
);
