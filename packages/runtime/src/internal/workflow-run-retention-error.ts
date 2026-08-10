import { Schema } from "effect";

export class WorkflowRunRetentionError extends Schema.TaggedError<WorkflowRunRetentionError>()(
  "WorkflowRunRetentionError",
  { cause: Schema.Defect() },
) {}
