import type { WorkflowRunAddress } from "@cvr/loom-domain";
import {
  LoomDynamicWorkflow,
  WorkflowRunRetention,
  WorkflowRunRetentionError,
  type WorkflowRunRetentionShape,
} from "@cvr/loom-runtime";
import { Effect, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { Client } from "effect-encore";

export const makeSqliteWorkflowRunRetention: Effect.Effect<
  WorkflowRunRetentionShape,
  never,
  Client | SqlClient.SqlClient
> = Effect.gen(function* () {
  const client = yield* Client;
  const sql = yield* SqlClient.SqlClient;

  const retire = Effect.fn("SqliteWorkflowRunRetention.retire")(
    function* ({ sessionId, workflowRunId }: WorkflowRunAddress) {
      yield* LoomDynamicWorkflow.prune(workflowRunId).pipe(Effect.provideService(Client, client));
      yield* sql`
        DELETE FROM workflow_signal_declarations
        WHERE workflow_run_id = ${workflowRunId}
      `;
      yield* sql`
        DELETE FROM workflow_run_acceptance
        WHERE session_id = ${sessionId} AND workflow_run_id = ${workflowRunId}
      `;
    },
    Effect.mapError((cause) => new WorkflowRunRetentionError({ cause })),
    client.withTransaction,
  );

  return WorkflowRunRetention.of({ retire });
});

export const layerSqliteWorkflowRunRetention = Layer.effect(
  WorkflowRunRetention,
  makeSqliteWorkflowRunRetention,
);
