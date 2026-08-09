import { Schema } from "effect";

export class ProcessInspectionError extends Schema.TaggedError<ProcessInspectionError>()(
  "ProcessInspectionError",
  {
    pid: Schema.Int,
    cause: Schema.Defect(),
  },
) {}
