import { RpcGroup } from "effect/unstable/rpc";
import { EvaluateCell } from "./evaluate-cell.js";
import { ExecuteWorkflow } from "./execute-workflow.js";
import { Handshake } from "./handshake.js";
import { ResetCodeKernel } from "./reset-code-kernel.js";

export class LoomRpcs extends RpcGroup.make(
  Handshake,
  EvaluateCell,
  ResetCodeKernel,
  ExecuteWorkflow,
) {}
