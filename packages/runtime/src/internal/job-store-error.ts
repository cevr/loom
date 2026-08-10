import { Schema } from "effect";

export class JobStoreError extends Schema.TaggedError<JobStoreError>()("JobStoreError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}
