import { SqliteClient } from "@effect/sql-sqlite-bun";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";

const createKernelSchema = Effect.gen(function* () {
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
  yield* sql`
    CREATE TABLE IF NOT EXISTS workflow_run_acceptance (
      session_id TEXT NOT NULL,
      workflow_name TEXT NOT NULL,
      workflow_version TEXT NOT NULL,
      workflow_key TEXT NOT NULL,
      workflow_run_id TEXT NOT NULL UNIQUE,
      request_digest TEXT NOT NULL,
      PRIMARY KEY (session_id, workflow_name, workflow_version, workflow_key)
    )
  `;
});

const createWorkflowSchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS workflow_signal_declarations (
      workflow_run_id TEXT NOT NULL,
      signal_name TEXT NOT NULL,
      PRIMARY KEY (workflow_run_id, signal_name)
    )
  `;
});

const createCapabilitySchema = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS workflow_child_agents (
      activity_key TEXT PRIMARY KEY,
      agent_id TEXT NOT NULL UNIQUE,
      session_id TEXT NOT NULL,
      workflow_run_id TEXT NOT NULL,
      prompt TEXT NOT NULL,
      status TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS workflow_child_agents_session
    ON workflow_child_agents (session_id, status)
  `;
  yield* sql`
    CREATE TABLE IF NOT EXISTS workflow_jobs (
      activity_key TEXT PRIMARY KEY,
      job_id TEXT NOT NULL UNIQUE,
      session_id TEXT NOT NULL,
      workflow_run_id TEXT NOT NULL,
      status TEXT NOT NULL
    )
  `;
});

const createSchema = Effect.all(
  [createKernelSchema, createWorkflowSchema, createCapabilitySchema],
  { discard: true },
);

export interface LoomSqliteConfig {
  readonly filename: string;
}

export const layerLoomSqlite = (config: LoomSqliteConfig) =>
  Layer.effectDiscard(createSchema).pipe(
    Layer.provideMerge(SqliteClient.layer({ filename: config.filename })),
  );
