import { WorkflowCapability, WorkflowStepId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowStepError extends Schema.TaggedError<WorkflowStepError>()(
  "WorkflowStepError",
  {
    stepId: WorkflowStepId,
    capability: WorkflowCapability,
    message: Schema.String,
  },
) {}
