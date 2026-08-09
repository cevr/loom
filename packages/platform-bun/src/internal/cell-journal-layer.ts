import { SqliteClient } from "@effect/sql-sqlite-bun";
import { CellJournal, type CellJournalStoreError } from "@cvr/loom-runtime";
import { Layer } from "effect";
import { layerSqliteCellJournal } from "./sqlite-cell-journal.js";

export interface CellJournalConfig {
  readonly filename: string;
}

export const layerCellJournal = (
  config: CellJournalConfig,
): Layer.Layer<CellJournal, CellJournalStoreError> =>
  layerSqliteCellJournal.pipe(Layer.provide(SqliteClient.layer({ filename: config.filename })));
