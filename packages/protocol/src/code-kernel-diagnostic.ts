import { Schema } from "effect";

export class CodeKernelDiagnostic extends Schema.Class<CodeKernelDiagnostic>(
  "@cvr/loom-protocol/CodeKernelDiagnostic",
)({
  requestId: Schema.optional(Schema.Natural),
  exitCode: Schema.optional(Schema.Int),
  stderrTail: Schema.optional(Schema.String),
  stderrPath: Schema.optional(Schema.String),
}) {}
