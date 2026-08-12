import { CellId } from "@cvr/loom-domain";
import { Effect, Schema } from "effect";
import { CellFileChange } from "./cell-file-change.js";

export class CellEvaluation extends Schema.Class<CellEvaluation>(
  "@cvr/loom-protocol/CellEvaluation",
)({
  cellId: CellId,
  display: Schema.String,
  bindings: Schema.Array(Schema.String),
  durationMillis: Schema.Finite.pipe(
    Schema.withDecodingDefaultTypeKey(Effect.succeed(0)),
    Schema.withConstructorDefault(Effect.succeed(0)),
  ),
  fileChanges: Schema.Array(CellFileChange).pipe(
    Schema.withDecodingDefaultTypeKey(Effect.succeed([])),
    Schema.withConstructorDefault(Effect.succeed([])),
  ),
}) {}
