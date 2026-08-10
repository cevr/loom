import { Schema } from "effect";

export class WorkflowInterpreterVersionMismatchError extends Schema.TaggedError<WorkflowInterpreterVersionMismatchError>()(
  "WorkflowInterpreterVersionMismatchError",
  {
    supported: Schema.Natural,
    received: Schema.Natural,
  },
) {}
