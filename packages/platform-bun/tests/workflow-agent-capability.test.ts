import { BunServices } from "@effect/platform-bun";
import {
  JobAddress,
  SessionId,
  WorkflowActivityKey,
  WorkflowCapability,
  WorkflowRunId,
  WorkflowStepId,
  WorkspaceRoot,
  workflowAgentJobId,
} from "@cvr/loom-domain";
import {
  JobRuntime,
  WorkflowActivityContext,
  WorkflowAgentResult,
  WorkflowCapabilityExecutor,
  WorkflowChildAgentStore,
  WorkflowStepCall,
  layerActorStateHub,
  layerSessionLifecycle,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Layer, Option, Schedule, Schema } from "effect";
import {
  layerBunJobRuntime,
  layerBunProcessController,
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteJobStore,
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
} from "../src/index.js";

const workflowAgentFixture = new URL("./fixtures/workflow-agent.ts", import.meta.url).pathname;

const context = WorkflowActivityContext.make({
  activityKey: WorkflowActivityKey.make("workflow/agent"),
  sessionId: SessionId.make("session-1"),
  workflowRunId: WorkflowRunId.make("workflow-1"),
});

const capabilityLayer = (
  filename: string,
  workspaceRoot: WorkspaceRoot,
  maximumOutputBytes = 64 * 1_024,
) => {
  const database = layerLoomSqlite({ filename });
  const agents = layerSqliteWorkflowChildAgentStore.pipe(Layer.provide(database));
  const store = layerSqliteJobStore.pipe(Layer.provide(database));
  const jobs = layerBunJobRuntime({ workspaceRoot, terminationGrace: "50 millis" }).pipe(
    Layer.provide([layerActorStateHub, layerBunProcessController, layerBunProcessInspector, store]),
  );
  const capabilities = layerWorkflowCapabilities({
    workspaceRoot,
    executable: "bun",
    arguments: ["run", workflowAgentFixture],
    maximumOutputBytes,
  }).pipe(Layer.provide([agents, jobs]), Layer.provideMerge(layerSessionLifecycle));
  return Layer.mergeAll(database, agents, store, jobs, capabilities);
};

it.scopedLive.layer(BunServices.layer)("cancels a Workflow Agent during compensation", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-agent-cancel-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const call = WorkflowStepCall.make({
      stepId: WorkflowStepId.make("agent-step"),
      capability: WorkflowCapability.make("agent"),
      input: { prompt: `wait-for:${directory}/release-agent` },
    });

    yield* Effect.gen(function* () {
      const executor = yield* WorkflowCapabilityExecutor;
      const runtime = yield* JobRuntime;
      const agents = yield* WorkflowChildAgentStore;
      const execution = yield* executor
        .execute(call, context)
        .pipe(Effect.forkChild({ startImmediately: true }));
      const address = JobAddress.make({
        sessionId: context.sessionId,
        jobId: workflowAgentJobId(context.activityKey),
      });
      yield* runtime.inspect(address).pipe(
        Effect.repeat({
          until: Option.exists((job) => job.status === "Running"),
          schedule: Schedule.spaced("10 millis"),
        }),
        Effect.timeout("5 seconds"),
      );

      yield* executor.compensate(call, context);
      const executionResult = yield* Fiber.join(execution);
      const result = yield* Schema.decodeUnknownEffect(WorkflowAgentResult)(executionResult.value);
      expect(result.outcome).toEqual({ _tag: "Cancelled" });
      expect(yield* agents.listActiveBySession(context.sessionId)).toEqual([]);
    }).pipe(Effect.provide(capabilityLayer(`${directory}/loom.sqlite`, workspaceRoot)));
  }),
);

it.scopedLive.layer(BunServices.layer)("replaces incomplete UTF-8 at the Agent output bound", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-agent-output-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const call = WorkflowStepCall.make({
      stepId: WorkflowStepId.make("agent-output-step"),
      capability: WorkflowCapability.make("agent"),
      input: { prompt: "😀".repeat(128) },
    });

    const result = yield* WorkflowCapabilityExecutor.pipe(
      Effect.flatMap((executor) => executor.execute(call, context)),
      Effect.flatMap((execution) =>
        Schema.decodeUnknownEffect(WorkflowAgentResult)(execution.value),
      ),
      Effect.provide(capabilityLayer(`${directory}/loom.sqlite`, workspaceRoot, 17)),
    );

    expect(result.outcome).toEqual({ _tag: "Succeeded", exitCode: 0 });
    expect(result.stdout).toBe("agent-complete:�");
  }),
);
