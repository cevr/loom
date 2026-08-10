import { Schema } from "effect";

export class WorkflowRunAcceptanceError extends Schema.TaggedError<WorkflowRunAcceptanceError>()(
  "WorkflowRunAcceptanceError",
  {
    operation: Schema.Literals(["claim", "digest", "list", "lookup", "mint"]),
    message: Schema.String,
  },
) {}
