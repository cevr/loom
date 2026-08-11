import { Schema } from "effect";
import { SessionClosingError } from "./session-closing-error.js";
import { PluginStateRevisionConflictError } from "./plugin-state-revision-conflict-error.js";
import { PluginStateStoreError } from "./plugin-state-store-error.js";

export const PluginStateWriteError = Schema.Union([
  PluginStateRevisionConflictError,
  PluginStateStoreError,
  SessionClosingError,
]);
export type PluginStateWriteError = typeof PluginStateWriteError.Type;
