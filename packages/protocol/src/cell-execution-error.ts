import { CellId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class CellExecutionError extends Schema.TaggedError<CellExecutionError>()(
  "CellExecutionError",
  {
    cellId: CellId,
    message: Schema.String,
  },
) {}
