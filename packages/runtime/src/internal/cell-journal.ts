import { type AgentOwner, type CellJournalEntry } from "@cvr/loom-domain";
import { Context, type Effect } from "effect";
import type { CellJournalStoreError } from "./cell-journal-store-error.js";

export interface CellJournalShape {
  readonly append: (entry: CellJournalEntry) => Effect.Effect<void, CellJournalStoreError>;
  readonly list: (
    owner: AgentOwner,
  ) => Effect.Effect<ReadonlyArray<CellJournalEntry>, CellJournalStoreError>;
}

export class CellJournal extends Context.Service<CellJournal, CellJournalShape>()(
  "@cvr/loom-runtime/CellJournal",
) {}
