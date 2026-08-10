import type { WorkflowRunId, WorkflowSignalName } from "@cvr/loom-domain";
import {
  WorkflowSignalDeclarations,
  WorkflowSignalDeclarationsError,
  type WorkflowSignalDeclarationsShape,
} from "@cvr/loom-runtime";
import { Effect, Inspectable, Layer } from "effect";
import { SqlClient } from "effect/unstable/sql";
import type { SqlError } from "effect/unstable/sql/SqlError";

const declarationsError = (
  operation: WorkflowSignalDeclarationsError["operation"],
  cause: SqlError,
) =>
  new WorkflowSignalDeclarationsError({ operation, message: Inspectable.toStringUnknown(cause) });

export const makeSqliteWorkflowSignalDeclarations: Effect.Effect<
  WorkflowSignalDeclarationsShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  const declare = Effect.fn("SqliteWorkflowSignalDeclarations.declare")(
    function* (workflowRunId: WorkflowRunId, names: ReadonlyArray<WorkflowSignalName>) {
      if (names.length === 0) return;
      yield* sql`
        INSERT OR IGNORE INTO workflow_signal_declarations
        ${sql.insert(names.map((name) => ({ workflow_run_id: workflowRunId, signal_name: name })))}
      `;
    },
    Effect.mapError((cause) => declarationsError("declare", cause)),
  );

  const contains = Effect.fn("SqliteWorkflowSignalDeclarations.contains")(
    function* (workflowRunId: WorkflowRunId, name: WorkflowSignalName) {
      const rows = yield* sql`
        SELECT 1
        FROM workflow_signal_declarations
        WHERE workflow_run_id = ${workflowRunId} AND signal_name = ${name}
        LIMIT 1
      `;
      return rows.length > 0;
    },
    Effect.mapError((cause) => declarationsError("contains", cause)),
  );

  return WorkflowSignalDeclarations.of({ declare, contains });
});

export const layerSqliteWorkflowSignalDeclarations = Layer.effect(
  WorkflowSignalDeclarations,
  makeSqliteWorkflowSignalDeclarations,
);
