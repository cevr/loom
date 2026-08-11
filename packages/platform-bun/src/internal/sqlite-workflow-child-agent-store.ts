import {
  WorkflowChildAgent,
  workflowAgentId,
  workflowAgentJobId,
  type SessionId,
  type WorkflowActivityKey,
  WorkflowRunAddress,
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
  ...WorkflowChildAgent.fields.parent.fields,
  prompt: WorkflowChildAgent.fields.prompt,
  status: WorkflowChildAgent.fields.status,
});

const toChildAgent = (row: typeof PersistedChildAgent.Type) =>
  WorkflowChildAgent.make({
    activityKey: row.activityKey,
    agentId: workflowAgentId(row.activityKey),
    jobId: workflowAgentJobId(row.activityKey),
    parent: WorkflowRunAddress.make({
      sessionId: row.sessionId,
      workflowRunId: row.workflowRunId,
    }),
    prompt: row.prompt,
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
      prompt: WorkflowChildAgent.fields.prompt,
    }),
    Result: PersistedChildAgent,
    execute: (claim) => sql`
      INSERT INTO workflow_child_agents (
        activity_key, session_id, workflow_run_id, prompt, status
      ) VALUES (
        ${claim.activityKey}, ${claim.sessionId}, ${claim.workflowRunId}, ${claim.prompt}, 'Active'
      )
      ON CONFLICT(activity_key) DO UPDATE SET activity_key = excluded.activity_key
      RETURNING
        activity_key AS activityKey, session_id AS sessionId,
        workflow_run_id AS workflowRunId, prompt, status
    `,
  });

const makeActiveRows = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Struct({ sessionId: WorkflowActivityContext.fields.sessionId }),
    Result: PersistedChildAgent,
    execute: ({ sessionId }) => sql`
      SELECT
        activity_key AS activityKey, session_id AS sessionId,
        workflow_run_id AS workflowRunId, prompt, status
      FROM workflow_child_agents
      WHERE session_id = ${sessionId} AND status = 'Active'
      ORDER BY activity_key
    `,
  });

const makeActiveWorkflowRows = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: WorkflowRunAddress,
    Result: PersistedChildAgent,
    execute: ({ sessionId, workflowRunId }) => sql`
      SELECT
        activity_key AS activityKey, session_id AS sessionId,
        workflow_run_id AS workflowRunId, prompt, status
      FROM workflow_child_agents
      WHERE session_id = ${sessionId}
        AND workflow_run_id = ${workflowRunId}
        AND status = 'Active'
      ORDER BY activity_key
    `,
  });

const makeClaim = (sql: SqlClient.SqlClient) => {
  const claimRow = makeClaimRow(sql);
  return Effect.fn("SqliteWorkflowChildAgentStore.claim")(
    function* (context: WorkflowActivityContext, prompt: string) {
      return toChildAgent(yield* claimRow({ ...context, prompt }));
    },
    Effect.catchTags({ NoSuchElementError: Effect.die, SchemaError: Effect.die }),
    Effect.mapError((cause) => storeError("claimAgent", cause)),
  );
};

const makeStop = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteWorkflowChildAgentStore.stop")(
    function* (activityKey: WorkflowActivityKey) {
      yield* sql`
        UPDATE workflow_child_agents SET status = 'Stopped' WHERE activity_key = ${activityKey}
      `;
    },
    Effect.asVoid,
    Effect.mapError((cause) => storeError("stopAgent", cause)),
  );

const makeListActiveBySession = (sql: SqlClient.SqlClient) => {
  const activeRows = makeActiveRows(sql);
  return Effect.fn("SqliteWorkflowChildAgentStore.listActiveBySession")(
    function* (sessionId: SessionId) {
      return (yield* activeRows({ sessionId })).map(toChildAgent);
    },
    Effect.catchTag("SchemaError", Effect.die),
    Effect.mapError((cause) => storeError("listActiveAgents", cause)),
  );
};

const makeStopSession = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteWorkflowChildAgentStore.stopSession")(
    function* (sessionId: SessionId) {
      yield* sql`
        UPDATE workflow_child_agents SET status = 'Stopped'
        WHERE session_id = ${sessionId} AND status = 'Active'
      `;
    },
    Effect.asVoid,
    Effect.mapError((cause) => storeError("stopSessionAgents", cause)),
  );

const makeListActiveByWorkflowRun = (sql: SqlClient.SqlClient) => {
  const activeRows = makeActiveWorkflowRows(sql);
  return Effect.fn("SqliteWorkflowChildAgentStore.listActiveByWorkflowRun")(
    function* (address: WorkflowRunAddress) {
      return (yield* activeRows(address)).map(toChildAgent);
    },
    Effect.catchTag("SchemaError", Effect.die),
    Effect.mapError((cause) => storeError("listActiveWorkflowAgents", cause)),
  );
};

export const makeSqliteWorkflowChildAgentStore: Effect.Effect<
  WorkflowChildAgentStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return WorkflowChildAgentStore.of({
    claim: makeClaim(sql),
    stop: makeStop(sql),
    listActiveBySession: makeListActiveBySession(sql),
    listActiveByWorkflowRun: makeListActiveByWorkflowRun(sql),
    stopSession: makeStopSession(sql),
  });
});

export const layerSqliteWorkflowChildAgentStore = Layer.effect(
  WorkflowChildAgentStore,
  makeSqliteWorkflowChildAgentStore,
);
