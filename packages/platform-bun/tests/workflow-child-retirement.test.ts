import {
  WorkflowActivityKey,
  WorkflowDefinition,
  WorkflowKey,
  type WorkflowRunRequest,
  WorkflowSignalName,
} from "@cvr/loom-domain";
import { WorkflowRunRecovery, WorkflowRuntime } from "@cvr/loom-runtime";
import { expect } from "effect-bun-test";
import { Effect, Exit, FileSystem, Ref } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { request } from "./workflow-runtime-fixtures.js";
import {
  claimWorkflowChildAgent,
  emptyWorkflowStorage,
  expireRetirement,
  leasedRuntime,
  retirementStatus,
  scopedLive,
  storageCounts,
} from "./workflow-runtime-test-support.js";

const testLayer = (filename: string, executions: Ref.Ref<number>) =>
  leasedRuntime(filename, executions, "1 hour");

const completeWithChild = (
  filename: string,
  executions: Ref.Ref<number>,
  workflowRequest: WorkflowRunRequest,
  activityKey: WorkflowActivityKey,
) =>
  Effect.gen(function* () {
    const runtime = yield* WorkflowRuntime;
    const workflowRunId = yield* runtime.send(workflowRequest);
    yield* claimWorkflowChildAgent(
      { sessionId: workflowRequest.sessionId, workflowRunId },
      activityKey,
      "Check Workflow retirement.",
    );
    yield* runtime.wait({ sessionId: workflowRequest.sessionId, workflowRunId });
    return workflowRunId;
  }).pipe(Effect.scoped, Effect.provide(testLayer(filename, executions)));

const resumeTwice = (filename: string, executions: Ref.Ref<number>) =>
  Effect.scoped(
    WorkflowRunRecovery.use((recovery) =>
      recovery.retire.pipe(Effect.andThen(recovery.retire), Effect.andThen(storageCounts)),
    ).pipe(Effect.provide(testLayer(filename, executions))),
  );

const failChildStop = (
  filename: string,
  executions: Ref.Ref<number>,
  workflowRunId: Parameters<typeof expireRetirement>[0],
) =>
  Effect.gen(function* () {
    yield* expireRetirement(workflowRunId);
    const sql = yield* SqlClient.SqlClient;
    yield* Effect.acquireRelease(
      sql`
          CREATE TRIGGER fail_workflow_child_stop
          BEFORE UPDATE OF status ON workflow_child_agents
          WHEN NEW.status = 'Stopped'
          BEGIN SELECT RAISE(ABORT, 'child stop failed'); END
        `,
      () => sql`DROP TRIGGER fail_workflow_child_stop`.pipe(Effect.orDie),
    );
    yield* WorkflowRunRecovery.use((recovery) => recovery.retire);
    expect(yield* retirementStatus(workflowRunId)).toBe("Retiring");
    expect(yield* storageCounts).toMatchObject({ acceptance: 1, activeAgents: 1 });
  }).pipe(Effect.scoped, Effect.provide(testLayer(filename, executions)));

scopedLive("resumes child Agent retirement after a stop failure and restart", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-child-retirement-" });
    const filename = `${directory}/loom.sqlite`;
    const executions = yield* Ref.make(0);
    const workflowRunId = yield* completeWithChild(
      filename,
      executions,
      request,
      WorkflowActivityKey.make("restart/child"),
    );

    yield* failChildStop(filename, executions, workflowRunId);
    expect(yield* resumeTwice(filename, executions)).toEqual(emptyWorkflowStorage);
  }),
);

const rollbackRequest = {
  ...request,
  key: WorkflowKey.make("retirement-rollback"),
  definition: WorkflowDefinition.make({
    ...request.definition,
    signals: [WorkflowSignalName.make("rollback")],
  }),
};

const failFinalRetirement = (
  filename: string,
  executions: Ref.Ref<number>,
  workflowRunId: Parameters<typeof expireRetirement>[0],
) =>
  Effect.gen(function* () {
    yield* expireRetirement(workflowRunId);
    const sql = yield* SqlClient.SqlClient;
    const before = yield* storageCounts;
    expect(before.messages + before.replies).toBeGreaterThan(0);
    yield* Effect.acquireRelease(
      sql`
          CREATE TRIGGER fail_signal_retirement
          BEFORE DELETE ON workflow_signal_declarations
          BEGIN SELECT RAISE(ABORT, 'signal retirement failed'); END
        `,
      () => sql`DROP TRIGGER fail_signal_retirement`.pipe(Effect.orDie),
    );
    const failed = yield* WorkflowRunRecovery.use((recovery) => recovery.retire).pipe(Effect.exit);
    expect(Exit.isFailure(failed)).toBe(true);
    expect(yield* retirementStatus(workflowRunId)).toBe("Retiring");
    expect(yield* storageCounts).toMatchObject({
      acceptance: 1,
      signals: 1,
      messages: before.messages,
      replies: before.replies,
      activeAgents: 0,
      stoppedAgents: 1,
    });
  }).pipe(Effect.scoped, Effect.provide(testLayer(filename, executions)));

scopedLive("rolls back Effect and Loom storage when final retirement fails", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-retirement-rollback-" });
    const filename = `${directory}/loom.sqlite`;
    const executions = yield* Ref.make(0);
    const workflowRunId = yield* completeWithChild(
      filename,
      executions,
      rollbackRequest,
      WorkflowActivityKey.make("rollback/child"),
    );

    yield* failFinalRetirement(filename, executions, workflowRunId);
    expect(yield* resumeTwice(filename, executions)).toEqual(emptyWorkflowStorage);
  }),
);
