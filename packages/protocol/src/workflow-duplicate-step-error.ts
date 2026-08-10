import { WorkflowStepId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowDuplicateStepError extends Schema.TaggedError<WorkflowDuplicateStepError>()(
  "WorkflowDuplicateStepError",
  { stepId: WorkflowStepId },
) {}
