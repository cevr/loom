import { RpcGroup } from "effect/unstable/rpc";
import { EvaluateCell } from "./evaluate-cell.js";
import { ExecuteWorkflow } from "./execute-workflow.js";
import { Handshake } from "./handshake.js";
import { ResetCodeKernel } from "./reset-code-kernel.js";
import { SignalWorkflow } from "./signal-workflow.js";
import { StartWorkflow } from "./start-workflow.js";

export class LoomRpcs extends RpcGroup.make(
  Handshake,
  EvaluateCell,
  ResetCodeKernel,
  StartWorkflow,
  ExecuteWorkflow,
  SignalWorkflow,
) {}
