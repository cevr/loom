import {
  JobProcessRecord,
  ProcessIdentity,
  type JobId,
  type JobProcessStatus,
} from "@cvr/loom-domain";
import {
  JobProcessStore,
  JobProcessStoreError,
  type JobProcessStoreShape,
} from "@cvr/loom-runtime";
import { Effect, Layer, Option, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";

const PersistedJobProcess = Schema.Struct({
  jobId: JobProcessRecord.fields.jobId,
  sessionId: JobProcessRecord.fields.sessionId,
  pid: ProcessIdentity.fields.pid,
  processGroupId: ProcessIdentity.fields.processGroupId,
  processStartId: ProcessIdentity.fields.processStartId,
  stdoutPath: JobProcessRecord.fields.stdoutPath,
  stderrPath: JobProcessRecord.fields.stderrPath,
  status: JobProcessRecord.fields.status,
  recoveryDetail: JobProcessRecord.fields.recoveryDetail,
});

const makeUpsert = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteJobProcessStore.upsert")(function* (record: JobProcessRecord) {
    yield* sql`
      INSERT INTO job_processes (
        job_id, session_id, pid, process_group_id, process_start_id,
        stdout_path, stderr_path, status, recovery_detail
      ) VALUES (
        ${record.jobId}, ${record.sessionId}, ${record.identity.pid},
        ${record.identity.processGroupId}, ${record.identity.processStartId},
        ${record.stdoutPath}, ${record.stderrPath}, ${record.status},
        ${Option.getOrNull(record.recoveryDetail)}
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
    `.pipe(
      Effect.asVoid,
      Effect.mapError((cause) => new JobProcessStoreError({ operation: "upsert", cause })),
    );
  });

const listRecoverable = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: PersistedJobProcess,
    execute: () => sql`
      SELECT
        job_id AS jobId, session_id AS sessionId, pid,
        process_group_id AS processGroupId, process_start_id AS processStartId,
        stdout_path AS stdoutPath, stderr_path AS stderrPath,
        status, recovery_detail AS recoveryDetail
      FROM job_processes
      WHERE status IN ('Running', 'Recovered')
      ORDER BY job_id
    `,
  });

const makeUpdateRecovery = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteJobProcessStore.updateRecovery")(function* (
    jobId: JobId,
    status: JobProcessStatus,
    detail: Option.Option<string>,
  ) {
    yield* sql`
      UPDATE job_processes
      SET status = ${status}, recovery_detail = ${Option.getOrNull(detail)}
      WHERE job_id = ${jobId}
    `.pipe(
      Effect.asVoid,
      Effect.mapError((cause) => new JobProcessStoreError({ operation: "updateRecovery", cause })),
    );
  });

export const makeSqliteJobProcessStore: Effect.Effect<
  JobProcessStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const list = listRecoverable(sql);
  return JobProcessStore.of({
    upsert: makeUpsert(sql),
    listRecoverable: list().pipe(
      Effect.map((rows) =>
        rows.map((row) =>
          JobProcessRecord.make({
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
        ),
      ),
      Effect.catchTag("SchemaError", Effect.die),
      Effect.mapError((cause) => new JobProcessStoreError({ operation: "listRecoverable", cause })),
    ),
    updateRecovery: makeUpdateRecovery(sql),
  });
});

export const layerSqliteJobProcessStore: Layer.Layer<JobProcessStore, never, SqlClient.SqlClient> =
  Layer.effect(JobProcessStore, makeSqliteJobProcessStore);
