import { CellId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class CellInterruptedError extends Schema.TaggedError<CellInterruptedError>()(
  "CellInterruptedError",
  {
    cellId: CellId,
    reason: Schema.Literals(["JournalFailure", "ProcessExited", "ProtocolFailure", "TimedOut"]),
    message: Schema.String,
  },
) {}
