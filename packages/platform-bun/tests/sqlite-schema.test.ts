import { BunServices } from "@effect/platform-bun";
import { expect, it } from "effect-bun-test";
import { Effect, Exit, FileSystem, Schema } from "effect";
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
        AND name IN (
          'cell_ledger',
          'code_kernel_processes',
          'jobs',
          'plugin_state',
          'workflow_run_acceptance'
        )
      ORDER BY name
    `,
  });
  const readCells = SqlSchema.findAll({
    Request: Schema.Void,
    Result: Cell,
    execute: () => sql`SELECT source FROM cell_ledger ORDER BY rowid`,
  });
  return {
    tables: (yield* listTables()).map(({ name }) => name),
    cells: yield* readCells(),
  };
});

const rejectPartialIdentity = (sql: SqlClient.SqlClient) =>
  Effect.exit(sql`
    INSERT INTO jobs (
      job_id, session_id, command, attached, status,
      stdout_path, stderr_path, result_path, pid
    ) VALUES (
      'job-1', 'session-1', 'sleep 30', 1, 'Starting',
      '/tmp/job-1/stdout.log', '/tmp/job-1/stderr.log', '/tmp/job-1/result', 42001
    )
  `);

const rejectRunningWithoutIdentity = (sql: SqlClient.SqlClient) =>
  Effect.exit(sql`
    INSERT INTO jobs (
      job_id, session_id, command, attached, status,
      stdout_path, stderr_path, result_path
    ) VALUES (
      'job-2', 'session-1', 'sleep 30', 1, 'Running',
      '/tmp/job-2/stdout.log', '/tmp/job-2/stderr.log', '/tmp/job-2/result'
    )
  `);

const rejectSuccessWithoutExitCode = (sql: SqlClient.SqlClient) =>
  Effect.exit(sql`
    INSERT INTO jobs (
      job_id, session_id, command, attached, status,
      stdout_path, stderr_path, result_path,
      pid, process_group_id, process_start_id
    ) VALUES (
      'job-3', 'session-1', 'sleep 30', 1, 'Succeeded',
      '/tmp/job-3/stdout.log', '/tmp/job-3/stderr.log', '/tmp/job-3/result',
      42003, 42003, 'Sun Aug  9 10:00:00 2026'
    )
  `);

scopedLive("creates the Loom schema and preserves current data", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-schema-" });
    const filename = `${directory}/loom.sqlite`;

    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        INSERT INTO cell_ledger (session_id, agent_id, cell_id, source, state_json)
        VALUES ('session-1', 'agent-1', 'cell-1', 'const answer = 42', '{"_tag":"Accepted"}')
      `;
    }).pipe(Effect.provide(layerLoomSqlite({ filename })), Effect.scoped);

    const expected = {
      tables: [
        "cell_ledger",
        "code_kernel_processes",
        "jobs",
        "plugin_state",
        "workflow_run_acceptance",
      ],
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

scopedLive("enforces durable Job state invariants", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-schema-job-" });

    const results = yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const partialIdentity = yield* rejectPartialIdentity(sql);
      const runningWithoutIdentity = yield* rejectRunningWithoutIdentity(sql);
      const successWithoutExitCode = yield* rejectSuccessWithoutExitCode(sql);
      return [partialIdentity, runningWithoutIdentity, successWithoutExitCode];
    }).pipe(
      Effect.provide(layerLoomSqlite({ filename: `${directory}/loom.sqlite` })),
      Effect.scoped,
    );

    expect(results.every(Exit.isFailure)).toBe(true);
  }),
);
