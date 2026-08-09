import { Schema } from "effect";

export class JobProcessStoreError extends Schema.TaggedError<JobProcessStoreError>()(
  "JobProcessStoreError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}
