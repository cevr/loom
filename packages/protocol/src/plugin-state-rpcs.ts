import { RpcGroup } from "effect/unstable/rpc";
import { ReadPluginState } from "./read-plugin-state.js";
import { WritePluginState } from "./write-plugin-state.js";

export class PluginStateRpcs extends RpcGroup.make(ReadPluginState, WritePluginState) {}
