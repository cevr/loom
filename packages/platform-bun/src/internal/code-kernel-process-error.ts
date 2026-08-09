import { Schema } from "effect";

export class CodeKernelProcessError extends Schema.TaggedError<CodeKernelProcessError>()(
  "CodeKernelProcessError",
  {
    reason: Schema.Literals(["ProcessExited", "ProtocolFailure", "TimedOut"]),
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}
