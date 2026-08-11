import { PluginStateAddress } from "@cvr/loom-domain";
import { Schema } from "effect";

export const PluginStateRevision = Schema.Int.check(Schema.isGreaterThan(0));
export type PluginStateRevision = typeof PluginStateRevision.Type;

export const PluginStateVersion = Schema.TaggedUnion({
  Missing: {},
  Present: { revision: PluginStateRevision },
});
export type PluginStateVersion = typeof PluginStateVersion.Type;

export const PluginStateReadResult = Schema.TaggedUnion({
  Missing: {},
  Present: {
    value: Schema.Json,
    revision: PluginStateRevision,
  },
});
export type PluginStateReadResult = typeof PluginStateReadResult.Type;

export const ReadPluginStateRequest = Schema.Struct({ address: PluginStateAddress });
export type ReadPluginStateRequest = typeof ReadPluginStateRequest.Type;

export const WritePluginStateRequest = Schema.Struct({
  address: PluginStateAddress,
  expected: PluginStateVersion,
  value: Schema.Json,
});
export type WritePluginStateRequest = typeof WritePluginStateRequest.Type;
