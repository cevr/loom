import { JobId, WorkflowJob, workflowJobId, type WorkflowActivityKey } from "@cvr/loom-domain";
import {
  WorkflowCapabilityStoreError,
  WorkflowJobStore,
  WorkflowActivityContext,
  type WorkflowJobStoreShape,
} from "@cvr/loom-runtime";
import { Effect, Inspectable, Layer, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";

const PersistedWorkflowJob = Schema.Struct({
  activityKey: WorkflowJob.fields.activityKey,
  jobId: WorkflowJob.fields.jobId,
  sessionId: WorkflowJob.fields.sessionId,
  workflowRunId: WorkflowJob.fields.workflowRunId,
  status: WorkflowJob.fields.status,
});

const storeError = (operation: string, cause: object) =>
  new WorkflowCapabilityStoreError({
    operation,
    message: Inspectable.toStringUnknown(cause),
  });

const makeClaimRow = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Schema.Struct({
      ...WorkflowActivityContext.fields,
      jobId: JobId,
    }),
    Result: PersistedWorkflowJob,
    execute: (claim) => sql`
      INSERT INTO workflow_jobs (
        activity_key, job_id, session_id, workflow_run_id, status
      ) VALUES (
        ${claim.activityKey}, ${claim.jobId}, ${claim.sessionId},
        ${claim.workflowRunId}, 'Accepted'
      )
      ON CONFLICT(activity_key) DO UPDATE SET activity_key = excluded.activity_key
      RETURNING
        activity_key AS activityKey, job_id AS jobId, session_id AS sessionId,
        workflow_run_id AS workflowRunId, status
    `,
  });

const begin = (sql: SqlClient.SqlClient, activityKey: WorkflowActivityKey) =>
  sql`
    UPDATE workflow_jobs SET status = 'Starting'
    WHERE activity_key = ${activityKey} AND status IN ('Accepted', 'Failed')
    RETURNING activity_key
  `.pipe(
    Effect.map((rows) => rows.length === 1),
    Effect.mapError((cause) => storeError("beginJob", cause)),
  );

const setStatus = (
  sql: SqlClient.SqlClient,
  operation: string,
  activityKey: WorkflowActivityKey,
  status: WorkflowJob["status"],
) =>
  sql`
    UPDATE workflow_jobs SET status = ${status}
    WHERE activity_key = ${activityKey} AND status IN ('Starting', 'Running')
  `.pipe(
    Effect.asVoid,
    Effect.mapError((cause) => storeError(operation, cause)),
  );

const failStarting = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: Schema.Struct({ jobId: JobId }),
    execute: () => sql`
      UPDATE workflow_jobs SET status = 'Failed'
      WHERE status = 'Starting'
      RETURNING job_id AS jobId
    `,
  })().pipe(
    Effect.map((rows) => rows.map((row) => row.jobId)),
    Effect.catchTag("SchemaError", Effect.die),
    Effect.mapError((cause) => storeError("failStartingJobs", cause)),
  );

export const makeSqliteWorkflowJobStore: Effect.Effect<
  WorkflowJobStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const claimRow = makeClaimRow(sql);

  const claim = Effect.fn("SqliteWorkflowJobStore.claim")(
    function* (context: WorkflowActivityContext) {
      return yield* claimRow({ ...context, jobId: workflowJobId(context.activityKey) });
    },
    Effect.catchTags({ NoSuchElementError: Effect.die, SchemaError: Effect.die }),
    Effect.mapError((cause) => storeError("claimJob", cause)),
  );

  return WorkflowJobStore.of({
    claim,
    begin: (activityKey) => begin(sql, activityKey),
    markRunning: (activityKey) => setStatus(sql, "markJobRunning", activityKey, "Running"),
    markFailed: (activityKey) => setStatus(sql, "markJobFailed", activityKey, "Failed"),
    failStarting: failStarting(sql),
  });
});

export const layerSqliteWorkflowJobStore = Layer.effect(
  WorkflowJobStore,
  makeSqliteWorkflowJobStore,
);
