import { Schema } from "effect";

export class CellJournalStoreError extends Schema.TaggedError<CellJournalStoreError>()(
  "CellJournalStoreError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}
