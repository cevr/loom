import { BunServices } from "@effect/platform-bun";
import {
  JobAddress,
  SessionId,
  WorkflowActivityKey,
  WorkflowCapability,
  WorkflowRunId,
  WorkflowStepId,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  JobRuntime,
  WorkflowActivityContext,
  WorkflowAgentHandle,
  WorkflowArtifactStore,
  WorkflowArtifactWrite,
  WorkflowCapabilityExecutor,
  WorkflowChildAgentStore,
  WorkflowJobHandle,
  WorkflowStepCall,
  layerActorStateHub,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer, Option, Schedule, Schema } from "effect";
import {
  layerBunJobRuntime,
  layerBunProcessController,
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteJobStore,
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
} from "../src/index.js";

const agentContext = WorkflowActivityContext.make({
  activityKey: WorkflowActivityKey.make("workflow/agent"),
  sessionId: SessionId.make("session-1"),
  workflowRunId: WorkflowRunId.make("workflow-1"),
});
const jobContext = WorkflowActivityContext.make({
  ...agentContext,
  activityKey: WorkflowActivityKey.make("workflow/job"),
});
const nextAgentContext = WorkflowActivityContext.make({
  ...agentContext,
  activityKey: WorkflowActivityKey.make("workflow-next/agent"),
  workflowRunId: WorkflowRunId.make("workflow-2"),
});
const nextJobContext = WorkflowActivityContext.make({
  ...nextAgentContext,
  activityKey: WorkflowActivityKey.make("workflow-next/job"),
});
const agentCall = WorkflowStepCall.make({
  stepId: WorkflowStepId.make("agent-step"),
  capability: WorkflowCapability.make("agent"),
  input: { prompt: "Check the build." },
});
const jobCall = WorkflowStepCall.make({
  stepId: WorkflowStepId.make("job-step"),
  capability: WorkflowCapability.make("job"),
  input: { command: ": > cwd-marker; printf 'job-finished\\n'" },
});

const waitForOutput = (fs: FileSystem.FileSystem, path: string, expected: string) =>
  fs.readFileString(path).pipe(
    Effect.retry(Schedule.spaced("10 millis")),
    Effect.repeat({
      while: (output) => output !== expected,
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.timeout("5 seconds"),
  );

const capabilityLayer = (filename: string, workspaceRoot: WorkspaceRoot) => {
  const database = layerLoomSqlite({ filename });
  const agents = layerSqliteWorkflowChildAgentStore.pipe(Layer.provide(database));
  const store = layerSqliteJobStore.pipe(Layer.provide(database));
  const actors = layerActorStateHub;
  const jobs = layerBunJobRuntime({ workspaceRoot, terminationGrace: "50 millis" }).pipe(
    Layer.provide([actors, layerBunProcessController, layerBunProcessInspector, store]),
  );
  const capabilities = layerWorkflowCapabilities({ workspaceRoot }).pipe(
    Layer.provide([agents, jobs]),
  );
  return Layer.mergeAll(database, agents, jobs, capabilities);
};

it.scopedLive.layer(BunServices.layer)("returns stable capability handles", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-capabilities-" });
    const workspaceRoot = WorkspaceRoot.make(directory);

    yield* Effect.gen(function* () {
      const executor = yield* WorkflowCapabilityExecutor;
      const artifacts = yield* WorkflowArtifactStore;
      const agents = yield* WorkflowChildAgentStore;
      const agentResults = yield* Effect.all(
        [executor.execute(agentCall, agentContext), executor.execute(agentCall, agentContext)],
        { concurrency: "unbounded" },
      );
      const jobResults = yield* Effect.all(
        [executor.execute(jobCall, jobContext), executor.execute(jobCall, jobContext)],
        { concurrency: "unbounded" },
      );

      expect(agentResults[0].value).toEqual(agentResults[1].value);
      const nextAgent = yield* executor.execute(agentCall, nextAgentContext);
      expect(nextAgent.value).not.toEqual(agentResults[0].value);
      expect(
        yield* Schema.decodeUnknownEffect(WorkflowAgentHandle)(agentResults[0].value),
      ).toBeDefined();
      expect(yield* agents.listActiveBySession(agentContext.sessionId)).toHaveLength(2);
      expect(jobResults[0].value).toEqual(jobResults[1].value);
      const nextJob = yield* executor.execute(jobCall, nextJobContext);
      expect(nextJob.value).not.toEqual(jobResults[0].value);

      const artifactWrite = WorkflowArtifactWrite.make({
        stepId: WorkflowStepId.make("artifact-step"),
        value: { result: "complete" },
      });
      const artifactResults = yield* Effect.all([
        artifacts.store(artifactWrite, agentContext),
        artifacts.store(artifactWrite, agentContext),
      ]);
      expect(artifactResults[0]).toEqual(artifactResults[1]);
      expect(yield* artifacts.store(artifactWrite, nextAgentContext)).not.toEqual(
        artifactResults[0],
      );

      const job = yield* Schema.decodeUnknownEffect(WorkflowJobHandle)(jobResults[0].value);
      const stdoutPath = `${directory}/.loom/jobs/${encodeURIComponent(job.jobId)}/stdout.log`;
      expect(yield* waitForOutput(fs, stdoutPath, "job-finished\n")).toBe("job-finished\n");
      expect(yield* fs.exists(`${directory}/cwd-marker`)).toBe(true);
    }).pipe(Effect.provide(capabilityLayer(`${directory}/loom.sqlite`, workspaceRoot)));
  }),
);

it.scopedLive.layer(BunServices.layer)("cancels a Workflow Job during compensation", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-capability-cancel-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const longCall = WorkflowStepCall.make({ ...jobCall, input: { command: "sleep 30" } });

    yield* Effect.gen(function* () {
      const executor = yield* WorkflowCapabilityExecutor;
      const runtime = yield* JobRuntime;
      const result = yield* executor.execute(longCall, jobContext);
      const handle = yield* Schema.decodeUnknownEffect(WorkflowJobHandle)(result.value);
      const address = JobAddress.make({ sessionId: jobContext.sessionId, jobId: handle.jobId });
      yield* runtime.inspect(address).pipe(
        Effect.repeat({
          until: Option.exists((job) => job.status === "Running"),
          schedule: Schedule.spaced("10 millis"),
        }),
        Effect.timeout("5 seconds"),
      );
      yield* executor.compensate(longCall, jobContext);
      expect(Option.map(yield* runtime.inspect(address), (job) => job.status)).toEqual(
        Option.some("Cancelled"),
      );
    }).pipe(Effect.provide(capabilityLayer(`${directory}/loom.sqlite`, workspaceRoot)));
  }),
);
