import type { SessionId } from "@cvr/loom-domain";
import { Context, type Duration, type Effect } from "effect";
import type { SessionClosureStoreError } from "./session-closure-store-error.js";

export interface SessionClosureStoreShape {
  readonly close: (
    sessionId: SessionId,
    lease: Duration.Input,
  ) => Effect.Effect<void, SessionClosureStoreError>;
  readonly contains: (sessionId: SessionId) => Effect.Effect<boolean, SessionClosureStoreError>;
  readonly list: Effect.Effect<ReadonlyArray<SessionId>, SessionClosureStoreError>;
  readonly prune: Effect.Effect<number, SessionClosureStoreError>;
}

export class SessionClosureStore extends Context.Service<
  SessionClosureStore,
  SessionClosureStoreShape
>()("@cvr/loom-runtime/SessionClosureStore") {}
