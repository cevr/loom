import { WorkflowSignalAddress } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowSignalNotDeclaredError extends Schema.TaggedError<WorkflowSignalNotDeclaredError>()(
  "WorkflowSignalNotDeclaredError",
  { address: WorkflowSignalAddress },
) {}
