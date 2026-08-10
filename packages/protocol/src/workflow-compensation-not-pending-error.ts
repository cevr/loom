import { WorkflowRunAddress } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowCompensationNotPendingError extends Schema.TaggedError<WorkflowCompensationNotPendingError>()(
  "WorkflowCompensationNotPendingError",
  { address: WorkflowRunAddress },
) {}
