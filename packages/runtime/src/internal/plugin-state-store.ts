import type { PluginStateAddress, SessionId } from "@cvr/loom-domain";
import type {
  PluginStateReadResult,
  PluginStateRevision,
  PluginStateStoreError,
  PluginStateWriteError,
  PluginStateVersion,
} from "@cvr/loom-protocol";
import { Context, type Effect, type Schema } from "effect";

export interface PluginStateStoreShape {
  readonly read: (
    address: PluginStateAddress,
  ) => Effect.Effect<PluginStateReadResult, PluginStateStoreError>;
  readonly write: (
    address: PluginStateAddress,
    expected: PluginStateVersion,
    value: Schema.Json,
  ) => Effect.Effect<PluginStateRevision, PluginStateWriteError>;
  readonly deleteSession: (sessionId: SessionId) => Effect.Effect<void, PluginStateStoreError>;
}

export class PluginStateStore extends Context.Service<PluginStateStore, PluginStateStoreShape>()(
  "@cvr/loom-runtime/PluginStateStore",
) {}
