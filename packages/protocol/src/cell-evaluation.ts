import { CellId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class CellEvaluation extends Schema.Class<CellEvaluation>(
  "@cvr/loom-protocol/CellEvaluation",
)({
  cellId: CellId,
  display: Schema.String,
  bindings: Schema.Array(Schema.String),
}) {}
