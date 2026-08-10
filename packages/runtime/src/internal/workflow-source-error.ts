import { Schema } from "effect";

export class WorkflowSourceError extends Schema.TaggedError<WorkflowSourceError>()(
  "WorkflowSourceError",
  { message: Schema.String },
) {}
