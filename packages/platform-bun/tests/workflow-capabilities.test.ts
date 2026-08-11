import { BunServices } from "@effect/platform-bun";
import {
  JobAddress,
  JobFailure,
  JobOutcome,
  JobSubmission,
  SessionId,
  WorkflowActivityKey,
  WorkflowCapability,
  WorkflowRunId,
  WorkflowStepId,
  WorkspaceRoot,
  workflowJobId,
} from "@cvr/loom-domain";
import {
  JobRuntime,
  JobStore,
  WorkflowActivityContext,
  WorkflowAgentHandle,
  WorkflowArtifactStore,
  WorkflowArtifactWrite,
  WorkflowCapabilityExecutor,
  WorkflowChildAgentStore,
  WorkflowJobHandle,
  WorkflowStepError,
  WorkflowStepCall,
  layerActorStateHub,
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
      expect((yield* executor.execute(jobCall, jobContext)).value).toEqual(jobResults[0].value);
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
      expect(yield* fs.readFileString(stdoutPath)).toBe("job-finished\n");
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
      const execution = yield* executor.execute(longCall, jobContext).pipe(Effect.forkChild);
      const address = JobAddress.make({
        sessionId: jobContext.sessionId,
        jobId: workflowJobId(jobContext.activityKey),
      });
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
      const error = yield* Fiber.join(execution).pipe(Effect.flip);
      expect(error).toBeInstanceOf(WorkflowStepError);
      expect(error.message).toBe("The Job was cancelled.");
    }).pipe(Effect.provide(capabilityLayer(`${directory}/loom.sqlite`, workspaceRoot)));
  }),
);

it.scopedLive.layer(BunServices.layer)("rejects a terminal failed Job launch", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-capability-failed-job-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobId = workflowJobId(jobContext.activityKey);

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.create(
        JobSubmission.make({
          jobId,
          sessionId: jobContext.sessionId,
          command: ": > cwd-marker; printf 'job-finished\\n'",
          attached: true,
          stdoutPath: `${directory}/.loom/jobs/${encodeURIComponent(jobId)}/stdout.log`,
          stderrPath: `${directory}/.loom/jobs/${encodeURIComponent(jobId)}/stderr.log`,
          resultPath: `${directory}/.loom/jobs/${encodeURIComponent(jobId)}/result`,
        }),
      );
      yield* jobs.begin(jobId);
      yield* jobs.complete(
        jobId,
        JobOutcome.cases.Failed.make({
          failure: JobFailure.cases.Runtime.make({ detail: "The process runtime failed." }),
        }),
      );
    }).pipe(Effect.provide(layerSqliteJobStore.pipe(Layer.provide(layerLoomSqlite({ filename })))));

    const error = yield* Effect.gen(function* () {
      const executor = yield* WorkflowCapabilityExecutor;
      return yield* executor.execute(jobCall, jobContext).pipe(Effect.flip);
    }).pipe(Effect.provide(capabilityLayer(filename, workspaceRoot)));
    expect(error).toBeInstanceOf(WorkflowStepError);
    expect(error.message).toBe("The process runtime failed.");
  }),
);

it.scopedLive.layer(BunServices.layer)("rejects a Job process that exits with failure", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({
      prefix: "loom-capability-process-failure-",
    });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const failedCall = WorkflowStepCall.make({ ...jobCall, input: { command: "exit 7" } });

    const error = yield* Effect.gen(function* () {
      const executor = yield* WorkflowCapabilityExecutor;
      return yield* executor.execute(failedCall, jobContext).pipe(Effect.flip);
    }).pipe(Effect.provide(capabilityLayer(`${directory}/loom.sqlite`, workspaceRoot)));

    expect(error).toBeInstanceOf(WorkflowStepError);
    expect(error.message).toBe("The Job exited with code 7.");
  }),
);
