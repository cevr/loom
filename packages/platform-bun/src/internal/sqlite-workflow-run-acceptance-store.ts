import {
  WorkflowIncarnationId,
  WorkflowIdentity,
  WorkflowRequestDigest,
  WorkflowRunAddress,
  WorkflowRunId,
} from "@cvr/loom-domain";
import {
  WorkflowRunAcceptanceError,
  WorkflowRunClaim,
  WorkflowRunAcceptanceStore,
  type WorkflowRunAcceptanceStoreShape,
} from "@cvr/loom-runtime";
import { Effect, Inspectable, Layer, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";

const Claim = Schema.Struct({
  ...WorkflowIdentity.fields,
  digest: WorkflowRequestDigest,
  incarnationId: WorkflowIncarnationId,
  workflowRunId: WorkflowRunId,
});

const makeClaim = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Claim,
    Result: WorkflowRunClaim,
    execute: (claim) =>
      sql`
        INSERT INTO workflow_run_acceptance (
          session_id, workflow_name, workflow_version, workflow_key,
          incarnation_id, workflow_run_id, request_digest
        ) VALUES (
          ${claim.sessionId}, ${claim.name}, ${claim.version}, ${claim.key},
          ${claim.incarnationId}, ${claim.workflowRunId}, ${claim.digest}
        )
        ON CONFLICT(session_id, workflow_name, workflow_version, workflow_key)
        DO UPDATE SET request_digest = workflow_run_acceptance.request_digest
        RETURNING
          incarnation_id AS incarnationId,
          workflow_run_id AS workflowRunId,
          request_digest AS digest
      `,
  });

const makeLookup = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOneOption({
    Request: WorkflowRunId,
    Result: WorkflowIdentity,
    execute: (workflowRunId) => sql`
      SELECT
        session_id AS sessionId,
        workflow_name AS name,
        workflow_version AS version,
        workflow_key AS key
      FROM workflow_run_acceptance
      WHERE workflow_run_id = ${workflowRunId}
    `,
  });

const makeList = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: WorkflowRunAddress,
    execute: () => sql`
      SELECT session_id AS sessionId, workflow_run_id AS workflowRunId
      FROM workflow_run_acceptance
      ORDER BY workflow_run_id
    `,
  });

const storeError = (operation: WorkflowRunAcceptanceError["operation"], cause: unknown) =>
  new WorkflowRunAcceptanceError({ operation, message: Inspectable.toStringUnknown(cause) });

export const makeSqliteWorkflowRunAcceptanceStore: Effect.Effect<
  WorkflowRunAcceptanceStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const claim = makeClaim(sql);
  const lookup = makeLookup(sql);
  const list = makeList(sql);
  return WorkflowRunAcceptanceStore.of({
    claim: (identity, digest, incarnationId, workflowRunId) =>
      claim({ ...identity, digest, incarnationId, workflowRunId }).pipe(
        Effect.catchTags({
          NoSuchElementError: Effect.die,
          SchemaError: Effect.die,
        }),
        Effect.tapError((cause) => Effect.logError("Workflow acceptance claim failed.", cause)),
        Effect.mapError((cause) => storeError("claim", cause)),
      ),
    lookup: (workflowRunId) =>
      lookup(workflowRunId).pipe(
        Effect.tapError((cause) => Effect.logError("Workflow acceptance lookup failed.", cause)),
        Effect.mapError((cause) => storeError("lookup", cause)),
      ),
    list: list().pipe(
      Effect.tapError((cause) => Effect.logError("Workflow acceptance list failed.", cause)),
      Effect.mapError((cause) => storeError("list", cause)),
    ),
  });
});

export const layerSqliteWorkflowRunAcceptanceStore: Layer.Layer<
  WorkflowRunAcceptanceStore,
  never,
  SqlClient.SqlClient
> = Layer.effect(WorkflowRunAcceptanceStore, makeSqliteWorkflowRunAcceptanceStore);
