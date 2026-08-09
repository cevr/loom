import { RpcGroup } from "effect/unstable/rpc";
import { EvaluateCell } from "./evaluate-cell.js";
import { ResetCodeKernel } from "./reset-code-kernel.js";

export class LoomRpcs extends RpcGroup.make(EvaluateCell, ResetCodeKernel) {}
