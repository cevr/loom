import { JobProcessRecord, type JobId, type JobProcessStatus } from "@cvr/loom-domain";
import {
  JobProcessStore,
  JobProcessStoreError,
  type JobProcessStoreShape,
} from "@cvr/loom-runtime";
import { Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql";

const PersistedJobProcess = Schema.Struct({
  jobId: Schema.String,
  sessionId: Schema.String,
  pid: Schema.Finite,
  processGroupId: Schema.Finite,
  processStartId: Schema.String,
  stdoutPath: Schema.String,
  stderrPath: Schema.String,
  status: Schema.String,
  recoveryDetail: Schema.NullOr(Schema.String),
});

const decodeRecords = Schema.decodeUnknownEffect(Schema.Array(PersistedJobProcess));
const decodeRecord = Schema.decodeUnknownEffect(JobProcessRecord);

const storeError = (operation: string) => (cause: unknown) =>
  new JobProcessStoreError({ operation, cause });

const initializeStore = (sql: SqlClient.SqlClient) =>
  sql`
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
  `.pipe(Effect.asVoid, Effect.mapError(storeError("initialize")));

const makeUpsert = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteJobProcessStore.upsert")(function* (record: JobProcessRecord) {
    yield* sql`
      INSERT INTO job_processes (
        job_id, session_id, pid, process_group_id, process_start_id,
        stdout_path, stderr_path, status, recovery_detail
      ) VALUES (
        ${record.jobId}, ${record.sessionId}, ${record.identity.pid},
        ${record.identity.processGroupId}, ${record.identity.processStartId},
        ${record.stdoutPath}, ${record.stderrPath}, ${record.status}, ${record.recoveryDetail}
      )
      ON CONFLICT(job_id) DO UPDATE SET
        session_id = excluded.session_id,
        pid = excluded.pid,
        process_group_id = excluded.process_group_id,
        process_start_id = excluded.process_start_id,
        stdout_path = excluded.stdout_path,
        stderr_path = excluded.stderr_path,
        status = excluded.status,
        recovery_detail = excluded.recovery_detail
    `.pipe(Effect.asVoid, Effect.mapError(storeError("upsert")));
  });

const listRecoverable = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const rows = yield* sql`
      SELECT
        job_id AS jobId, session_id AS sessionId, pid,
        process_group_id AS processGroupId, process_start_id AS processStartId,
        stdout_path AS stdoutPath, stderr_path AS stderrPath,
        status, recovery_detail AS recoveryDetail
      FROM job_processes
      WHERE status IN ('Running', 'Stopping', 'Recovered')
      ORDER BY job_id
    `;
    const persisted = yield* decodeRecords(rows);
    return yield* Effect.forEach(persisted, (row) =>
      decodeRecord({
        jobId: row.jobId,
        sessionId: row.sessionId,
        identity: {
          pid: row.pid,
          processGroupId: row.processGroupId,
          processStartId: row.processStartId,
        },
        stdoutPath: row.stdoutPath,
        stderrPath: row.stderrPath,
        status: row.status,
        recoveryDetail: row.recoveryDetail,
      }),
    );
  }).pipe(Effect.mapError(storeError("listRecoverable")));

const makeUpdateRecovery = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteJobProcessStore.updateRecovery")(function* (
    jobId: JobId,
    status: JobProcessStatus,
    detail: string | null,
  ) {
    yield* sql`
      UPDATE job_processes
      SET status = ${status}, recovery_detail = ${detail}
      WHERE job_id = ${jobId}
    `.pipe(Effect.asVoid, Effect.mapError(storeError("updateRecovery")));
  });

export const makeSqliteJobProcessStore: Effect.Effect<
  JobProcessStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return JobProcessStore.of({
    initialize: initializeStore(sql),
    upsert: makeUpsert(sql),
    listRecoverable: listRecoverable(sql),
    updateRecovery: makeUpdateRecovery(sql),
  });
});

export const layerSqliteJobProcessStore: Layer.Layer<JobProcessStore, never, SqlClient.SqlClient> =
  Layer.effect(JobProcessStore, makeSqliteJobProcessStore);
