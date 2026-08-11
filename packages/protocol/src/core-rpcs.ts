import { RpcGroup } from "effect/unstable/rpc";
import { WatchActorStates } from "./actor-state.js";
import { CloseSession } from "./close-session.js";
import { EvaluateCell } from "./evaluate-cell.js";
import { Handshake } from "./handshake.js";
import { ResetCodeKernel } from "./reset-code-kernel.js";

export class CoreRpcs extends RpcGroup.make(
  Handshake,
  CloseSession,
  WatchActorStates,
  EvaluateCell,
  ResetCodeKernel,
) {}
