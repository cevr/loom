import { CodeKernelProcessRecord } from "@cvr/loom-domain";
import { Schema } from "effect";

export class CodeKernelProcessRecoveryError extends Schema.TaggedError<CodeKernelProcessRecoveryError>()(
  "CodeKernelProcessRecoveryError",
  {
    operation: Schema.Literals(["inspect", "terminate", "confirm", "remove"]),
    record: CodeKernelProcessRecord,
    cause: Schema.Defect(),
  },
) {}
