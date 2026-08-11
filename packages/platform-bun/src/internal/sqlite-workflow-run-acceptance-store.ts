import {
  WorkflowIncarnationId,
  WorkflowIdentity,
  WorkflowRunAcceptanceStatus,
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
import { Effect, Inspectable, Layer, Option, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";

const Claim = Schema.Struct({
  ...WorkflowIdentity.fields,
  digest: WorkflowRequestDigest,
  incarnationId: WorkflowIncarnationId,
  workflowRunId: WorkflowRunId,
});

const PersistedClaim = Schema.Struct({
  incarnationId: WorkflowIncarnationId,
  workflowRunId: WorkflowRunId,
  digest: WorkflowRequestDigest,
  status: WorkflowRunAcceptanceStatus,
});

const makeInsert = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOneOption({
    Request: Claim,
    Result: WorkflowRunClaim.cases.Claimed,
    execute: (claim) =>
      sql`
        INSERT INTO workflow_run_acceptance (
          session_id, workflow_name, workflow_version, workflow_key,
          incarnation_id, workflow_run_id, request_digest
        ) VALUES (
          ${claim.sessionId}, ${claim.name}, ${claim.version}, ${claim.key},
          ${claim.incarnationId}, ${claim.workflowRunId}, ${claim.digest}
        )
        ON CONFLICT(session_id, workflow_name, workflow_version, workflow_key) DO NOTHING
        RETURNING
          'Claimed' AS _tag,
          incarnation_id AS incarnationId,
          workflow_run_id AS workflowRunId,
          request_digest AS digest
      `,
  });

const makeExistingClaim = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: WorkflowIdentity,
    Result: PersistedClaim,
    execute: (identity) => sql`
      SELECT
        incarnation_id AS incarnationId,
        workflow_run_id AS workflowRunId,
        request_digest AS digest,
        status
      FROM workflow_run_acceptance
      WHERE session_id = ${identity.sessionId}
        AND workflow_name = ${identity.name}
        AND workflow_version = ${identity.version}
        AND workflow_key = ${identity.key}
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

const makeList = (sql: SqlClient.SqlClient, status: WorkflowRunAcceptanceStatus) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: WorkflowRunAddress,
    execute: () => sql`
      SELECT session_id AS sessionId, workflow_run_id AS workflowRunId
      FROM workflow_run_acceptance
      WHERE status = ${status}
      ORDER BY workflow_run_id
    `,
  });

const storeError = (operation: WorkflowRunAcceptanceError["operation"], cause: unknown) =>
  new WorkflowRunAcceptanceError({ operation, message: Inspectable.toStringUnknown(cause) });

const makeClaim = (sql: SqlClient.SqlClient) => {
  const insert = makeInsert(sql);
  const existingClaim = makeExistingClaim(sql);
  return (
    identity: WorkflowIdentity,
    digest: WorkflowRequestDigest,
    incarnationId: WorkflowIncarnationId,
    workflowRunId: WorkflowRunId,
  ) =>
    Effect.gen(function* () {
      const inserted = yield* insert({ ...identity, digest, incarnationId, workflowRunId });
      if (Option.isSome(inserted)) return inserted.value;
      const accepted = yield* existingClaim(identity);
      if (accepted.status === "Retiring") {
        return WorkflowRunClaim.cases.Retiring.make({ workflowRunId: accepted.workflowRunId });
      }
      return WorkflowRunClaim.cases.Claimed.make(accepted);
    }).pipe(
      sql.withTransaction,
      Effect.catchTags({ NoSuchElementError: Effect.die, SchemaError: Effect.die }),
      Effect.tapError((cause) => Effect.logError("Workflow acceptance claim failed.", cause)),
      Effect.mapError((cause) => storeError("claim", cause)),
    );
};

const makeMarkRetiring =
  (sql: SqlClient.SqlClient) =>
  ({ sessionId, workflowRunId }: WorkflowRunAddress) =>
    sql`
      UPDATE workflow_run_acceptance
      SET status = 'Retiring'
      WHERE session_id = ${sessionId} AND workflow_run_id = ${workflowRunId}
    `.pipe(
      sql.withTransaction,
      Effect.asVoid,
      Effect.tapError((cause) => Effect.logError("Workflow retirement mark failed.", cause)),
      Effect.mapError((cause) => storeError("retire", cause)),
    );

const mapListError = (label: string) =>
  Effect.tapError((cause) => Effect.logError(`${label} Workflow acceptance list failed.`, cause));

export const makeSqliteWorkflowRunAcceptanceStore: Effect.Effect<
  WorkflowRunAcceptanceStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const lookup = makeLookup(sql);
  const listActive = makeList(sql, "Active");
  const listRetiring = makeList(sql, "Retiring");
  return WorkflowRunAcceptanceStore.of({
    claim: makeClaim(sql),
    markRetiring: makeMarkRetiring(sql),
    lookup: (workflowRunId) =>
      lookup(workflowRunId).pipe(
        Effect.tapError((cause) => Effect.logError("Workflow acceptance lookup failed.", cause)),
        Effect.mapError((cause) => storeError("lookup", cause)),
      ),
    listActive: listActive().pipe(
      mapListError("Active"),
      Effect.mapError((cause) => storeError("list", cause)),
    ),
    listRetiring: listRetiring().pipe(
      mapListError("Retiring"),
      Effect.mapError((cause) => storeError("list", cause)),
    ),
  });
});

export const layerSqliteWorkflowRunAcceptanceStore: Layer.Layer<
  WorkflowRunAcceptanceStore,
  never,
  SqlClient.SqlClient
> = Layer.effect(WorkflowRunAcceptanceStore, makeSqliteWorkflowRunAcceptanceStore);
