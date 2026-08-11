import { Rpc } from "effect/unstable/rpc";
import { PluginStateReadResult, ReadPluginStateRequest } from "./plugin-state.js";
import { PluginStateStoreError } from "./plugin-state-store-error.js";

export class ReadPluginState extends Rpc.make("PluginState.Read", {
  payload: ReadPluginStateRequest,
  success: PluginStateReadResult,
  error: PluginStateStoreError,
}) {}
