import { Schema } from "effect";

export class JobRuntimeError extends Schema.TaggedError<JobRuntimeError>()("JobRuntimeError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}
