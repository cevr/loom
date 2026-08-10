import { WorkflowRunAddress, WorkflowStepId } from "@cvr/loom-domain";
import { Schema } from "effect";
import { WorkflowCompensationDecision } from "./workflow-compensation-decision.js";

export class WorkflowCompensationDecisionConflictError extends Schema.TaggedError<WorkflowCompensationDecisionConflictError>()(
  "WorkflowCompensationDecisionConflictError",
  {
    address: WorkflowRunAddress,
    stepId: WorkflowStepId,
    attempt: Schema.Int.check(Schema.isGreaterThan(0)),
    acceptedDecision: Schema.Option(WorkflowCompensationDecision),
  },
) {}
