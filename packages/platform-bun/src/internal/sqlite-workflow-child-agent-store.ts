import {
  AgentId,
  AgentParent,
  WorkflowChildAgent,
  type SessionId,
  type WorkflowActivityKey,
} from "@cvr/loom-domain";
import {
  WorkflowCapabilityStoreError,
  WorkflowChildAgentStore,
  WorkflowActivityContext,
  type WorkflowChildAgentStoreShape,
} from "@cvr/loom-runtime";
import { Effect, Inspectable, Layer, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";

const PersistedChildAgent = Schema.Struct({
  activityKey: WorkflowChildAgent.fields.activityKey,
  agentId: WorkflowChildAgent.fields.agentId,
  sessionId: WorkflowChildAgent.fields.parent.cases.WorkflowRun.fields.sessionId,
  workflowRunId: WorkflowChildAgent.fields.parent.cases.WorkflowRun.fields.workflowRunId,
  status: WorkflowChildAgent.fields.status,
});

const toChildAgent = (row: typeof PersistedChildAgent.Type) =>
  WorkflowChildAgent.make({
    activityKey: row.activityKey,
    agentId: row.agentId,
    parent: AgentParent.cases.WorkflowRun.make({
      sessionId: row.sessionId,
      workflowRunId: row.workflowRunId,
    }),
    status: row.status,
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
      agentId: AgentId,
    }),
    Result: PersistedChildAgent,
    execute: (claim) => sql`
      INSERT INTO workflow_child_agents (
        activity_key, agent_id, session_id, workflow_run_id, status
      ) VALUES (
        ${claim.activityKey}, ${claim.agentId}, ${claim.sessionId}, ${claim.workflowRunId}, 'Active'
      )
      ON CONFLICT(activity_key) DO UPDATE SET activity_key = excluded.activity_key
      RETURNING
        activity_key AS activityKey, agent_id AS agentId, session_id AS sessionId,
        workflow_run_id AS workflowRunId, status
    `,
  });

const makeActiveRows = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Struct({ sessionId: WorkflowActivityContext.fields.sessionId }),
    Result: PersistedChildAgent,
    execute: ({ sessionId }) => sql`
      SELECT
        activity_key AS activityKey, agent_id AS agentId, session_id AS sessionId,
        workflow_run_id AS workflowRunId, status
      FROM workflow_child_agents
      WHERE session_id = ${sessionId} AND status = 'Active'
      ORDER BY agent_id
    `,
  });

export const makeSqliteWorkflowChildAgentStore: Effect.Effect<
  WorkflowChildAgentStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const claimRow = makeClaimRow(sql);
  const activeRows = makeActiveRows(sql);

  const claim = Effect.fn("SqliteWorkflowChildAgentStore.claim")(
    function* (context: WorkflowActivityContext) {
      const row = yield* claimRow({ ...context, agentId: AgentId.make(context.activityKey) });
      return toChildAgent(row);
    },
    Effect.catchTags({ NoSuchElementError: Effect.die, SchemaError: Effect.die }),
    Effect.mapError((cause) => storeError("claimAgent", cause)),
  );

  const stop = Effect.fn("SqliteWorkflowChildAgentStore.stop")(
    function* (activityKey: WorkflowActivityKey) {
      yield* sql`
        UPDATE workflow_child_agents SET status = 'Stopped' WHERE activity_key = ${activityKey}
      `;
    },
    Effect.asVoid,
    Effect.mapError((cause) => storeError("stopAgent", cause)),
  );

  const listActiveBySession = Effect.fn("SqliteWorkflowChildAgentStore.listActiveBySession")(
    function* (sessionId: SessionId) {
      return (yield* activeRows({ sessionId })).map(toChildAgent);
    },
    Effect.catchTag("SchemaError", Effect.die),
    Effect.mapError((cause) => storeError("listActiveAgents", cause)),
  );

  return WorkflowChildAgentStore.of({ claim, stop, listActiveBySession });
});

export const layerSqliteWorkflowChildAgentStore = Layer.effect(
  WorkflowChildAgentStore,
  makeSqliteWorkflowChildAgentStore,
);
