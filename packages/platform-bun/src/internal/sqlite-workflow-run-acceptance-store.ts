import { WorkflowIdentity, WorkflowRequestDigest } from "@cvr/loom-domain";
import {
  WorkflowRunAcceptanceError,
  WorkflowRunAcceptanceStore,
  type WorkflowRunAcceptanceStoreShape,
} from "@cvr/loom-runtime";
import { Effect, Layer, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";

const Claim = Schema.Struct({
  ...WorkflowIdentity.fields,
  digest: WorkflowRequestDigest,
});

const DigestRow = Schema.Struct({ digest: WorkflowRequestDigest });

const initializeStore = (sql: SqlClient.SqlClient) =>
  sql`
    CREATE TABLE IF NOT EXISTS workflow_run_acceptance (
      session_id TEXT NOT NULL,
      workflow_name TEXT NOT NULL,
      workflow_version TEXT NOT NULL,
      workflow_key TEXT NOT NULL,
      request_digest TEXT NOT NULL,
      PRIMARY KEY (session_id, workflow_name, workflow_version, workflow_key)
    )
  `.pipe(
    Effect.asVoid,
    Effect.mapError((cause) => new WorkflowRunAcceptanceError({ operation: "initialize", cause })),
  );

const makeClaim = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Claim,
    Result: DigestRow,
    execute: (claim) =>
      sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`
            INSERT INTO workflow_run_acceptance (
              session_id, workflow_name, workflow_version, workflow_key, request_digest
            ) VALUES (
              ${claim.sessionId}, ${claim.name}, ${claim.version}, ${claim.key}, ${claim.digest}
            )
            ON CONFLICT(session_id, workflow_name, workflow_version, workflow_key) DO NOTHING
          `;
          return yield* sql`
            SELECT request_digest AS digest
            FROM workflow_run_acceptance
            WHERE session_id = ${claim.sessionId}
              AND workflow_name = ${claim.name}
              AND workflow_version = ${claim.version}
              AND workflow_key = ${claim.key}
          `;
        }),
      ),
  });

export const makeSqliteWorkflowRunAcceptanceStore: Effect.Effect<
  WorkflowRunAcceptanceStoreShape,
  WorkflowRunAcceptanceError,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* initializeStore(sql);
  const claim = makeClaim(sql);
  return WorkflowRunAcceptanceStore.of({
    claim: (identity, digest) =>
      claim({ ...identity, digest }).pipe(
        Effect.map(({ digest: acceptedDigest }) => acceptedDigest),
        // A successful transactional insert guarantees one valid digest row.
        Effect.catchTags({
          NoSuchElementError: Effect.die,
          SchemaError: Effect.die,
        }),
        Effect.mapError((cause) => new WorkflowRunAcceptanceError({ operation: "claim", cause })),
      ),
  });
});

export const layerSqliteWorkflowRunAcceptanceStore: Layer.Layer<
  WorkflowRunAcceptanceStore,
  WorkflowRunAcceptanceError,
  SqlClient.SqlClient
> = Layer.effect(WorkflowRunAcceptanceStore, makeSqliteWorkflowRunAcceptanceStore);
