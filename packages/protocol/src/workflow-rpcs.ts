import { RpcGroup } from "effect/unstable/rpc";
import { ExecuteWorkflow } from "./execute-workflow.js";
import { StartWorkflow } from "./start-workflow.js";
import { SignalWorkflow } from "./signal-workflow.js";
import { InspectWorkflow, InterruptWorkflow } from "./workflow-control.js";
import { DecideWorkflowCompensation } from "./workflow-compensation-control.js";

export class WorkflowRpcs extends RpcGroup.make(
  StartWorkflow,
  ExecuteWorkflow,
  SignalWorkflow,
  InspectWorkflow,
  InterruptWorkflow,
  DecideWorkflowCompensation,
) {}
