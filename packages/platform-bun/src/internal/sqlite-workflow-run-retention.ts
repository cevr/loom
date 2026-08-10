import { WorkflowRunAddress } from "@cvr/loom-domain";
import {
  LoomDynamicWorkflow,
  WorkflowRunRetention,
  WorkflowRunRetentionError,
  type WorkflowRunRetentionShape,
} from "@cvr/loom-runtime";
import { DateTime, type Duration, Effect, Layer, Option, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";
import { Client } from "effect-encore";

const ScheduledRetirement = Schema.Struct({ retireAfter: Schema.DateTimeUtcFromMillis });

const ScheduleRetirement = Schema.Struct({
  ...WorkflowRunAddress.fields,
  retireAfter: Schema.DateTimeUtcFromMillis,
});

const makeDeadline = (sql: SqlClient.SqlClient) => {
  const schedule = SqlSchema.findOneOption({
    Request: ScheduleRetirement,
    Result: ScheduledRetirement,
    execute: ({ sessionId, workflowRunId, retireAfter }) => sql`
      UPDATE workflow_run_acceptance
      SET retire_after = COALESCE(retire_after, ${retireAfter})
      WHERE session_id = ${sessionId} AND workflow_run_id = ${workflowRunId}
      RETURNING retire_after AS retireAfter
    `,
  });
  return Effect.fn("SqliteWorkflowRunRetention.deadline")(function* (
    address: WorkflowRunAddress,
    stateLease: Duration.Input,
  ) {
    const now = yield* DateTime.now;
    return yield* schedule({
      ...address,
      retireAfter: DateTime.addDuration(now, stateLease),
    });
  });
};

const makeRetire = (client: Client["Service"], sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteWorkflowRunRetention.retire")(function* ({
    sessionId,
    workflowRunId,
  }: WorkflowRunAddress) {
    yield* LoomDynamicWorkflow.prune(workflowRunId).pipe(Effect.provideService(Client, client));
    yield* sql`
        DELETE FROM workflow_signal_declarations
        WHERE workflow_run_id = ${workflowRunId}
      `;
    yield* sql`
        DELETE FROM workflow_run_acceptance
        WHERE session_id = ${sessionId} AND workflow_run_id = ${workflowRunId}
      `;
  }, client.withTransaction);

export const makeSqliteWorkflowRunRetention: Effect.Effect<
  WorkflowRunRetentionShape,
  never,
  Client | SqlClient.SqlClient
> = Effect.gen(function* () {
  const client = yield* Client;
  const sql = yield* SqlClient.SqlClient;
  const deadline = makeDeadline(sql);
  const retire = makeRetire(client, sql);
  const retireIfExpired = Effect.fn("SqliteWorkflowRunRetention.retireIfExpired")(function* (
    address: WorkflowRunAddress,
    retireAfter: DateTime.Utc,
  ) {
    if (!(yield* DateTime.isPast(retireAfter))) return false;
    yield* retire(address);
    return true;
  });

  const retireExpired = Effect.fn("SqliteWorkflowRunRetention.retireExpired")(
    function* (address: WorkflowRunAddress, stateLease: Duration.Input) {
      const scheduled = yield* deadline(address, stateLease);
      if (Option.isNone(scheduled)) return;
      yield* retireIfExpired(address, scheduled.value.retireAfter);
    },
    Effect.mapError((cause) => new WorkflowRunRetentionError({ cause })),
  );

  const retireAfterLease = Effect.fn("SqliteWorkflowRunRetention.retireAfterLease")(
    function* (address: WorkflowRunAddress, stateLease: Duration.Input) {
      const scheduled = yield* deadline(address, stateLease);
      if (Option.isNone(scheduled)) return;
      if (yield* retireIfExpired(address, scheduled.value.retireAfter)) return;
      const now = yield* DateTime.now;
      yield* Effect.sleep(DateTime.distance(now, scheduled.value.retireAfter));
      yield* retire(address);
    },
    Effect.mapError((cause) => new WorkflowRunRetentionError({ cause })),
  );

  return WorkflowRunRetention.of({ retireAfterLease, retireExpired });
});

export const layerSqliteWorkflowRunRetention = Layer.effect(
  WorkflowRunRetention,
  makeSqliteWorkflowRunRetention,
);
