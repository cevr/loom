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

const makeClaim = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Claim,
    Result: DigestRow,
    execute: (claim) =>
      sql`
        INSERT INTO workflow_run_acceptance (
          session_id, workflow_name, workflow_version, workflow_key, request_digest
        ) VALUES (
          ${claim.sessionId}, ${claim.name}, ${claim.version}, ${claim.key}, ${claim.digest}
        )
        ON CONFLICT(session_id, workflow_name, workflow_version, workflow_key)
        DO UPDATE SET request_digest = workflow_run_acceptance.request_digest
        RETURNING request_digest AS digest
      `,
  });

export const makeSqliteWorkflowRunAcceptanceStore: Effect.Effect<
  WorkflowRunAcceptanceStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const claim = makeClaim(sql);
  return WorkflowRunAcceptanceStore.of({
    claim: (identity, digest) =>
      claim({ ...identity, digest }).pipe(
        Effect.map(({ digest: acceptedDigest }) => acceptedDigest),
        // The upsert always returns the immutable accepted digest.
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
  never,
  SqlClient.SqlClient
> = Layer.effect(WorkflowRunAcceptanceStore, makeSqliteWorkflowRunAcceptanceStore);
