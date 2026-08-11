import { ActorStateProjection, SessionId } from "@cvr/loom-domain";
import { Schema } from "effect";
import { Rpc } from "effect/unstable/rpc";

export const ActorStateSnapshot = Schema.Array(ActorStateProjection);
export type ActorStateSnapshot = typeof ActorStateSnapshot.Type;

export const WatchActorStates = Rpc.make("ActorState.Watch", {
  payload: { sessionId: SessionId },
  success: ActorStateSnapshot,
  stream: true,
});
