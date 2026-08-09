import { Schema } from "effect";
import { CodeKernelDiagnostic } from "@cvr/loom-protocol";

export class CodeKernelProcessError extends Schema.TaggedError<CodeKernelProcessError>()(
  "CodeKernelProcessError",
  {
    reason: Schema.Literals(["ProcessExited", "ProtocolFailure", "TimedOut", "CrashLoop"]),
    message: Schema.String,
    cause: Schema.Defect(),
    diagnostic: Schema.optional(CodeKernelDiagnostic),
  },
) {}
