import { Schema } from "effect";

export class CellFileChange extends Schema.Class<CellFileChange>(
  "@cvr/loom-protocol/CellFileChange",
)({
  path: Schema.NonEmptyString,
  oldText: Schema.String,
  newText: Schema.String,
}) {}
