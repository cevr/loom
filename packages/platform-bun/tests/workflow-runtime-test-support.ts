import { BunCrypto, BunServices } from "@effect/platform-bun";
import {
  ArtifactId,
  WorkflowActivityKey,
  WorkflowRunAcceptanceStatus,
  type WorkflowRunAddress,
  WorkflowRunId,
} from "@cvr/loom-domain";
import {
  WorkflowActivityContext,
  WorkflowArtifactReference,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  WorkflowChildAgentStore,
  layerActorStateHub,
  WorkflowStepExecution,
} from "@cvr/loom-runtime";
import { it } from "effect-bun-test";
import { type Duration, Effect, Layer, Ref, Schema } from "effect";
import { SingleRunner } from "effect/unstable/cluster";
import { SqlClient, SqlSchema } from "effect/unstable/sql";
import {
  layerLoomSqlite,
  layerLoomWorkflowRuntime,
  layerLoomWorkflowRuntimeWith,
  layerSqliteWorkflowChildAgentStore,
} from "../src/index.js";

export const workflowSupport = (filename: string, executions: Ref.Ref<number>) => {
  const foundation = Layer.merge(layerLoomSqlite({ filename }), BunCrypto.layer);
  const capabilities = Layer.succeed(
    WorkflowCapabilityExecutor,
    WorkflowCapabilityExecutor.of({
      supports: () => true,
      execute: (call) =>
        Ref.update(executions, (count) => count + 1).pipe(
          Effect.as(
            WorkflowStepExecution.make({
              value: call.input,
              tokenCount: 0,
              agentRuns: 0,
            }),
          ),
        ),
      compensate: () => Effect.void,
    }),
  );
  const artifacts = Layer.succeed(
    WorkflowArtifactStore,
    WorkflowArtifactStore.of({
      store: ({ stepId }) =>
        Effect.succeed(
          WorkflowArtifactReference.make({
            artifactId: ArtifactId.make(`artifact-${stepId}`),
          }),
        ),
    }),
  );
  return Layer.mergeAll(
    foundation,
    SingleRunner.layer({
      runnerStorage: "memory",
      shardingConfig: { entityMessagePollInterval: "10 millis" },
    }).pipe(Layer.provide(foundation)),
    capabilities,
    artifacts,
  );
};

export const runtimeLayer = (
  filename: string,
  executions: Ref.Ref<number>,
  runtime: ReturnType<typeof layerLoomWorkflowRuntimeWith> = layerLoomWorkflowRuntime,
) => {
  const actors = layerActorStateHub;
  const support = workflowSupport(filename, executions);
  const childAgents = layerSqliteWorkflowChildAgentStore.pipe(Layer.provide(support));
  const provided = runtime.pipe(Layer.provide([support, actors, childAgents]));
  return Layer.mergeAll(provided, actors, support, childAgents);
};

export const leasedRuntime = (
  filename: string,
  executions: Ref.Ref<number>,
  stateLease: Duration.Input,
) => runtimeLayer(filename, executions, layerLoomWorkflowRuntimeWith({ stateLease }));

export const scopedLive = it.scopedLive.layer(BunServices.layer);

export const emptyWorkflowStorage = {
  acceptance: 0,
  signals: 0,
  messages: 0,
  replies: 0,
  activeAgents: 0,
  stoppedAgents: 0,
};

export const claimWorkflowChildAgent = (
  address: WorkflowRunAddress,
  activityKey: WorkflowActivityKey,
  prompt: string,
) =>
  WorkflowChildAgentStore.use((agents) =>
    agents.claim(WorkflowActivityContext.make({ ...address, activityKey }), prompt),
  );

const Count = Schema.Struct({ count: Schema.Finite });
const Retirement = Schema.Struct({
  retireAfter: Schema.OptionFromNullOr(Schema.DateTimeUtcFromMillis),
});
const RetirementStatus = Schema.Struct({ status: WorkflowRunAcceptanceStatus });

export const retirementDeadline = (workflowRunId: WorkflowRunId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const find = SqlSchema.findOne({
      Request: WorkflowRunId,
      Result: Retirement,
      execute: (id) => sql`
        SELECT retire_after AS retireAfter
        FROM workflow_run_acceptance
        WHERE workflow_run_id = ${id}
      `,
    });
    return yield* find(workflowRunId).pipe(Effect.catchTag("SchemaError", Effect.die));
  });

export const expireRetirement = (workflowRunId: WorkflowRunId) =>
  SqlClient.SqlClient.use(
    (sql) => sql`
    UPDATE workflow_run_acceptance
    SET retire_after = 0
    WHERE workflow_run_id = ${workflowRunId}
  `,
  ).pipe(Effect.asVoid);

export const retirementStatus = (workflowRunId: WorkflowRunId) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const find = SqlSchema.findOne({
      Request: WorkflowRunId,
      Result: RetirementStatus,
      execute: (id) => sql`
        SELECT status FROM workflow_run_acceptance WHERE workflow_run_id = ${id}
      `,
    });
    return (yield* find(workflowRunId)).status;
  });

const orchestrationCounts = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const countAcceptance = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Count,
      execute: () => sql`SELECT COUNT(*) AS count FROM workflow_run_acceptance`,
    });
    const countSignals = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Count,
      execute: () => sql`SELECT COUNT(*) AS count FROM workflow_signal_declarations`,
    });
    const countMessages = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Count,
      execute: () => sql`SELECT COUNT(*) AS count FROM cluster_messages`,
    });
    const countReplies = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Count,
      execute: () => sql`SELECT COUNT(*) AS count FROM cluster_replies`,
    });
    const [acceptance, signals, messages, replies] = yield* Effect.all([
      countAcceptance(),
      countSignals(),
      countMessages(),
      countReplies(),
    ]);
    return {
      acceptance: acceptance.count,
      signals: signals.count,
      messages: messages.count,
      replies: replies.count,
    };
  });

const childAgentCounts = (sql: SqlClient.SqlClient) =>
  Effect.gen(function* () {
    const countActiveAgents = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Count,
      execute: () => sql`
      SELECT COUNT(*) AS count FROM workflow_child_agents WHERE status = 'Active'
    `,
    });
    const countStoppedAgents = SqlSchema.findOne({
      Request: Schema.Void,
      Result: Count,
      execute: () => sql`
      SELECT COUNT(*) AS count FROM workflow_child_agents WHERE status = 'Stopped'
    `,
    });
    const [activeAgents, stoppedAgents] = yield* Effect.all([
      countActiveAgents(),
      countStoppedAgents(),
    ]);
    return { activeAgents: activeAgents.count, stoppedAgents: stoppedAgents.count };
  });

export const storageCounts = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const [orchestration, childAgents] = yield* Effect.all([
    orchestrationCounts(sql),
    childAgentCounts(sql),
  ]);
  return { ...orchestration, ...childAgents };
});
