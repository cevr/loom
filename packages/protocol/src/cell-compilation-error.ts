import { CellId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class CellCompilationError extends Schema.TaggedError<CellCompilationError>()(
  "CellCompilationError",
  {
    cellId: CellId,
    message: Schema.String,
  },
) {}
