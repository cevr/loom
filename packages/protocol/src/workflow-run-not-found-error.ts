import { WorkflowRunAddress } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowRunNotFoundError extends Schema.TaggedError<WorkflowRunNotFoundError>()(
  "WorkflowRunNotFoundError",
  { address: WorkflowRunAddress },
) {}
