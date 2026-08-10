import { SqliteClient, SqliteMigrator } from "@effect/sql-sqlite-bun";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";

const loader = SqliteMigrator.fromRecord({
  "0001_create_cell_journal": Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      CREATE TABLE IF NOT EXISTS cell_journal (
        journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
        session_id TEXT NOT NULL,
        agent_id TEXT NOT NULL,
        cell_id TEXT NOT NULL,
        source TEXT NOT NULL
      )
    `;
  }),
  "0002_create_job_processes": Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      CREATE TABLE IF NOT EXISTS job_processes (
        job_id TEXT PRIMARY KEY,
        session_id TEXT NOT NULL,
        pid INTEGER NOT NULL,
        process_group_id INTEGER NOT NULL,
        process_start_id TEXT NOT NULL,
        stdout_path TEXT NOT NULL,
        stderr_path TEXT NOT NULL,
        status TEXT NOT NULL,
        recovery_detail TEXT
      )
    `;
  }),
  "0003_create_workflow_run_acceptance": Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      CREATE TABLE IF NOT EXISTS workflow_run_acceptance (
        session_id TEXT NOT NULL,
        workflow_name TEXT NOT NULL,
        workflow_version TEXT NOT NULL,
        workflow_key TEXT NOT NULL,
        request_digest TEXT NOT NULL,
        PRIMARY KEY (session_id, workflow_name, workflow_version, workflow_key)
      )
    `;
  }),
});

const layerMigrations = SqliteMigrator.layer({ loader, table: "loom_migrations" });

export interface LoomSqliteConfig {
  readonly filename: string;
}

export const layerLoomSqlite = (config: LoomSqliteConfig) =>
  layerMigrations.pipe(Layer.provideMerge(SqliteClient.layer({ filename: config.filename })));
