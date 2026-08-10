import { Schema } from "effect";

export class WorkflowSignalDeclarationsError extends Schema.TaggedError<WorkflowSignalDeclarationsError>()(
  "WorkflowSignalDeclarationsError",
  {
    operation: Schema.Literals(["declare", "contains"]),
    message: Schema.String,
  },
) {}
