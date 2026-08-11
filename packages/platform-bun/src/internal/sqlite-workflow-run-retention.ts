import { JobAddress, WorkflowRunAddress } from "@cvr/loom-domain";
import {
  JobRuntime,
  LoomDynamicWorkflow,
  WorkflowChildAgentStore,
  WorkflowRunAcceptanceStore,
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

const RetiredWorkflowRun = Schema.Struct({
  workflowRunId: WorkflowRunAddress.fields.workflowRunId,
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

const makeFinalizeRetirement = (client: Client["Service"], sql: SqlClient.SqlClient) => {
  const removeAcceptance = SqlSchema.findOneOption({
    Request: WorkflowRunAddress,
    Result: RetiredWorkflowRun,
    execute: ({ sessionId, workflowRunId }) => sql`
      DELETE FROM workflow_run_acceptance
      WHERE session_id = ${sessionId}
        AND workflow_run_id = ${workflowRunId}
        AND NOT EXISTS (
          SELECT 1 FROM workflow_child_agents
          WHERE session_id = ${sessionId}
            AND workflow_run_id = ${workflowRunId}
            AND status = 'Active'
        )
      RETURNING workflow_run_id AS workflowRunId
    `,
  });
  return Effect.fn("SqliteWorkflowRunRetention.finalize")(function* (address: WorkflowRunAddress) {
    const removed = yield* removeAcceptance(address);
    if (Option.isNone(removed)) return;
    yield* LoomDynamicWorkflow.prune(address.workflowRunId).pipe(
      Effect.provideService(Client, client),
    );
    yield* sql`
        DELETE FROM workflow_signal_declarations
        WHERE workflow_run_id = ${address.workflowRunId}
      `;
    yield* sql`
        DELETE FROM workflow_child_agents
        WHERE session_id = ${address.sessionId}
          AND workflow_run_id = ${address.workflowRunId}
          AND status = 'Stopped'
      `;
  }, client.withTransaction);
};

const makeResume = (
  childAgents: WorkflowChildAgentStore["Service"],
  jobs: JobRuntime["Service"],
  finalize: ReturnType<typeof makeFinalizeRetirement>,
) =>
  Effect.fn("SqliteWorkflowRunRetention.resume")(function* (address: WorkflowRunAddress) {
    const active = yield* childAgents.listActiveByWorkflowRun(address);
    yield* Effect.forEach(
      active,
      (agent) =>
        jobs
          .cancel(JobAddress.make({ sessionId: address.sessionId, jobId: agent.jobId }))
          .pipe(Effect.andThen(childAgents.stop(agent.activityKey))),
      { discard: true },
    );
    yield* finalize(address);
  });

const makeRetire = (
  acceptance: WorkflowRunAcceptanceStore["Service"],
  resume: ReturnType<typeof makeResume>,
) =>
  Effect.fn("SqliteWorkflowRunRetention.retire")(function* (address: WorkflowRunAddress) {
    yield* acceptance.markRetiring(address);
    yield* resume(address);
  });

const makeRetireIfExpired = (retire: ReturnType<typeof makeRetire>) =>
  Effect.fn("SqliteWorkflowRunRetention.retireIfExpired")(function* (
    address: WorkflowRunAddress,
    retireAfter: DateTime.Utc,
  ) {
    if (!(yield* DateTime.isPast(retireAfter))) return false;
    yield* retire(address);
    return true;
  });

export const makeSqliteWorkflowRunRetention: Effect.Effect<
  WorkflowRunRetentionShape,
  never,
  Client | JobRuntime | SqlClient.SqlClient | WorkflowChildAgentStore | WorkflowRunAcceptanceStore
> = Effect.gen(function* () {
  const client = yield* Client;
  const sql = yield* SqlClient.SqlClient;
  const acceptance = yield* WorkflowRunAcceptanceStore;
  const childAgents = yield* WorkflowChildAgentStore;
  const jobs = yield* JobRuntime;
  const deadline = makeDeadline(sql);
  const finalizeRetirement = makeFinalizeRetirement(client, sql);
  const resume = makeResume(childAgents, jobs, finalizeRetirement);
  const retire = makeRetire(acceptance, resume);
  const retireIfExpired = makeRetireIfExpired(retire);

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

  const resumeRetirement = (address: WorkflowRunAddress) =>
    resume(address).pipe(Effect.mapError((cause) => new WorkflowRunRetentionError({ cause })));

  return WorkflowRunRetention.of({ resumeRetirement, retireAfterLease, retireExpired });
});

export const layerSqliteWorkflowRunRetention = Layer.effect(
  WorkflowRunRetention,
  makeSqliteWorkflowRunRetention,
);
