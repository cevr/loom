import type { WorkflowCapability } from "@cvr/loom-domain";
import type { WorkflowRunError } from "@cvr/loom-protocol";
import { Context, type Effect } from "effect";
import type { WorkflowStepCall, WorkflowStepExecution } from "@cvr/loom-protocol";
import type { WorkflowActivityContext } from "./workflow-capability-model.js";

export interface WorkflowCapabilityExecutorShape {
  readonly supports: (capability: WorkflowCapability) => boolean;
  readonly execute: (
    call: WorkflowStepCall,
    context: WorkflowActivityContext,
  ) => Effect.Effect<WorkflowStepExecution, WorkflowRunError>;
  readonly compensate: (
    call: WorkflowStepCall,
    context: WorkflowActivityContext,
  ) => Effect.Effect<void, WorkflowRunError>;
}

export class WorkflowCapabilityExecutor extends Context.Service<
  WorkflowCapabilityExecutor,
  WorkflowCapabilityExecutorShape
>()("@cvr/loom-runtime/WorkflowCapabilityExecutor") {}
