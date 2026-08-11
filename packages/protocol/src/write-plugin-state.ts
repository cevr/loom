import { Rpc } from "effect/unstable/rpc";
import { PluginStateRevision, WritePluginStateRequest } from "./plugin-state.js";
import { PluginStateWriteError } from "./plugin-state-write-error.js";

export class WritePluginState extends Rpc.make("PluginState.Write", {
  payload: WritePluginStateRequest,
  success: PluginStateRevision,
  error: PluginStateWriteError,
}) {}
