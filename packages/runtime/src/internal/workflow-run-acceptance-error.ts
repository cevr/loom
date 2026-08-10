import { Schema } from "effect";

export class WorkflowRunAcceptanceError extends Schema.TaggedError<WorkflowRunAcceptanceError>()(
  "WorkflowRunAcceptanceError",
  {
    operation: Schema.Literals(["initialize", "claim", "digest"]),
    cause: Schema.Defect(),
  },
) {}
