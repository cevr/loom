import { BunServices } from "@effect/platform-bun";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";
import { layerLoomSqlite } from "../src/index.js";

const Table = Schema.Struct({ name: Schema.String });
const Cell = Schema.Struct({ source: Schema.String });
const scopedLive = it.scopedLive.layer(BunServices.layer);

const inspectDatabase = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const listTables = SqlSchema.findAll({
    Request: Schema.Void,
    Result: Table,
    execute: () => sql`
      SELECT name
      FROM sqlite_schema
      WHERE type = 'table'
        AND name IN ('cell_journal', 'job_processes', 'workflow_run_acceptance')
      ORDER BY name
    `,
  });
  const readCells = SqlSchema.findAll({
    Request: Schema.Void,
    Result: Cell,
    execute: () => sql`SELECT source FROM cell_journal ORDER BY journal_id`,
  });
  return {
    tables: (yield* listTables()).map(({ name }) => name),
    cells: yield* readCells(),
  };
});

scopedLive("creates the Loom schema and preserves existing data", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-schema-" });
    const filename = `${directory}/loom.sqlite`;

    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        CREATE TABLE cell_journal (
          journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
          session_id TEXT NOT NULL,
          agent_id TEXT NOT NULL,
          cell_id TEXT NOT NULL,
          source TEXT NOT NULL
        )
      `;
      yield* sql`
        INSERT INTO cell_journal (session_id, agent_id, cell_id, source)
        VALUES ('session-1', 'agent-1', 'cell-1', 'const answer = 42')
      `;
    }).pipe(Effect.provide(SqliteClient.layer({ filename })), Effect.scoped);

    const expected = {
      tables: ["cell_journal", "job_processes", "workflow_run_acceptance"],
      cells: [{ source: "const answer = 42" }],
    };

    expect(
      yield* inspectDatabase.pipe(Effect.provide(layerLoomSqlite({ filename })), Effect.scoped),
    ).toEqual(expected);
    expect(
      yield* inspectDatabase.pipe(Effect.provide(layerLoomSqlite({ filename })), Effect.scoped),
    ).toEqual(expected);
  }),
);
