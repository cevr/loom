import { CellJournalEntry, type AgentOwner } from "@cvr/loom-domain";
import { CellJournal, CellJournalStoreError, type CellJournalShape } from "@cvr/loom-runtime";
import { Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql";

const PersistedCellJournalEntry = Schema.Struct({
  sessionId: Schema.String,
  agentId: Schema.String,
  cellId: Schema.String,
  source: Schema.String,
});

const decodeEntries = Schema.decodeUnknownEffect(Schema.Array(PersistedCellJournalEntry));
const decodeEntry = Schema.decodeUnknownEffect(CellJournalEntry);

const storeError = (operation: string) => (cause: unknown) =>
  new CellJournalStoreError({ operation, cause });

const initializeStore = (sql: SqlClient.SqlClient) =>
  sql`
    CREATE TABLE IF NOT EXISTS cell_journal (
      journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      agent_id TEXT NOT NULL,
      cell_id TEXT NOT NULL,
      source TEXT NOT NULL
    )
  `.pipe(Effect.asVoid, Effect.mapError(storeError("initialize")));

const makeAppend = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteCellJournal.append")(function* (entry: CellJournalEntry) {
    yield* sql`
      INSERT INTO cell_journal (session_id, agent_id, cell_id, source)
      VALUES (${entry.sessionId}, ${entry.agentId}, ${entry.cellId}, ${entry.source})
    `.pipe(Effect.asVoid, Effect.mapError(storeError("append")));
  });

const makeList = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteCellJournal.list")((owner: AgentOwner) =>
    Effect.gen(function* () {
      const rows = yield* sql`
        SELECT
          session_id AS sessionId,
          agent_id AS agentId,
          cell_id AS cellId,
          source
        FROM cell_journal
        WHERE session_id = ${owner.sessionId} AND agent_id = ${owner.agentId}
        ORDER BY journal_id
      `;
      const persisted = yield* decodeEntries(rows);
      return yield* Effect.forEach(persisted, (entry) => decodeEntry(entry));
    }).pipe(Effect.mapError(storeError("list"))),
  );

export const makeSqliteCellJournal: Effect.Effect<
  CellJournalShape,
  CellJournalStoreError,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* initializeStore(sql);
  return CellJournal.of({
    append: makeAppend(sql),
    list: makeList(sql),
  });
});

export const layerSqliteCellJournal: Layer.Layer<
  CellJournal,
  CellJournalStoreError,
  SqlClient.SqlClient
> = Layer.effect(CellJournal, makeSqliteCellJournal);
