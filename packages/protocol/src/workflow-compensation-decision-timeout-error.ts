import { WorkflowRunAddress } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowCompensationDecisionTimeoutError extends Schema.TaggedError<WorkflowCompensationDecisionTimeoutError>()(
  "WorkflowCompensationDecisionTimeoutError",
  { address: WorkflowRunAddress },
) {}
