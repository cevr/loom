import { AgentOwner, CellJournalEntry } from "@cvr/loom-domain";
import { CellJournal, CellJournalStoreError, type CellJournalShape } from "@cvr/loom-runtime";
import { Effect, Layer } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";

const makeAppend = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteCellJournal.append")(function* (entry: CellJournalEntry) {
    yield* sql`
      INSERT INTO cell_journal (session_id, agent_id, cell_id, source)
      VALUES (${entry.sessionId}, ${entry.agentId}, ${entry.cellId}, ${entry.source})
    `.pipe(
      Effect.asVoid,
      Effect.mapError((cause) => new CellJournalStoreError({ operation: "append", cause })),
    );
  });

const makeList = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: AgentOwner,
    Result: CellJournalEntry,
    execute: (owner) => sql`
      SELECT
        session_id AS sessionId,
        agent_id AS agentId,
        cell_id AS cellId,
        source
      FROM cell_journal
      WHERE session_id = ${owner.sessionId} AND agent_id = ${owner.agentId}
      ORDER BY journal_id
    `,
  });

export const makeSqliteCellJournal: Effect.Effect<CellJournalShape, never, SqlClient.SqlClient> =
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const list = makeList(sql);
    return CellJournal.of({
      append: makeAppend(sql),
      list: (owner) =>
        list(owner).pipe(
          Effect.catchTag("SchemaError", Effect.die),
          Effect.mapError((cause) => new CellJournalStoreError({ operation: "list", cause })),
        ),
    });
  });

export const layerSqliteCellJournal: Layer.Layer<CellJournal, never, SqlClient.SqlClient> =
  Layer.effect(CellJournal, makeSqliteCellJournal);
