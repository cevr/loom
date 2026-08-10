import { CellId } from "@cvr/loom-domain";
import { Schema } from "effect";
import { CodeKernelDiagnostic } from "./code-kernel-diagnostic.js";

export class CellInterruptedError extends Schema.TaggedError<CellInterruptedError>()(
  "CellInterruptedError",
  {
    cellId: CellId,
    reason: Schema.Literals([
      "JournalFailure",
      "ProcessExited",
      "ProcessIdentityFailure",
      "ProtocolFailure",
      "TimedOut",
      "CrashLoop",
      "DaemonRestart",
      "EvaluationInProgress",
    ]),
    message: Schema.String,
    diagnostic: Schema.optional(CodeKernelDiagnostic),
  },
) {}
