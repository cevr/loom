import { Schema } from "effect";

export class CellLedgerStoreError extends Schema.TaggedError<CellLedgerStoreError>()(
  "CellLedgerStoreError",
  {
    operation: Schema.Literals(["claim", "transition", "reconcile"]),
    cause: Schema.Defect(),
  },
) {}
