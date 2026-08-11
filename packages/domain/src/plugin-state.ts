import { Schema } from "effect";
import { PluginId, PluginStateKey, SessionId } from "./identifiers.js";

export const PluginStateScope = Schema.TaggedUnion({
  Workspace: {},
  Session: { sessionId: SessionId },
});
export type PluginStateScope = typeof PluginStateScope.Type;

export const PluginStateAddress = Schema.Struct({
  pluginId: PluginId,
  scope: PluginStateScope,
  key: PluginStateKey,
});
export type PluginStateAddress = typeof PluginStateAddress.Type;
