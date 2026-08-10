import { Schema } from "effect";

export class ProcessControllerError extends Schema.TaggedError<ProcessControllerError>()(
  "ProcessControllerError",
  {
    processGroupId: Schema.Int,
    signal: Schema.Literals(["SIGTERM", "SIGKILL"]),
    cause: Schema.Defect(),
  },
) {}
