import { RpcGroup } from "effect/unstable/rpc";
import { EvaluateCell } from "./evaluate-cell.js";
import { ExecuteWorkflow } from "./execute-workflow.js";
import { Handshake } from "./handshake.js";
import { InspectWorkflow, InterruptWorkflow } from "./workflow-control.js";
import { DecideWorkflowCompensation } from "./workflow-compensation-control.js";
import { CloseSession } from "./close-session.js";
import { ResetCodeKernel } from "./reset-code-kernel.js";
import { SignalWorkflow } from "./signal-workflow.js";
import { StartWorkflow } from "./start-workflow.js";

export class LoomRpcs extends RpcGroup.make(
  Handshake,
  CloseSession,
  EvaluateCell,
  ResetCodeKernel,
  StartWorkflow,
  ExecuteWorkflow,
  SignalWorkflow,
  InspectWorkflow,
  InterruptWorkflow,
  DecideWorkflowCompensation,
) {}
