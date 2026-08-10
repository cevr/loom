import { BunServices } from "@effect/platform-bun";
import {
  SessionId,
  WorkflowActivityKey,
  WorkflowCapability,
  WorkflowRunId,
  WorkflowStepId,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  ProcessInspectionError,
  ProcessInspector,
  ProcessObservation,
  layerJobReconciler,
  WorkflowActivityContext,
  WorkflowArtifactStore,
  WorkflowArtifactWrite,
  WorkflowCapabilityExecutor,
  WorkflowChildAgentStore,
  WorkflowJobHandle,
  WorkflowStepCall,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Deferred, Effect, Fiber, FileSystem, Layer, Option, Ref, Schedule, Schema } from "effect";
import {
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteJobProcessStore,
  layerSqliteWorkflowChildAgentStore,
  layerSqliteWorkflowJobStore,
  layerWorkflowCapabilities,
  makeBunProcessInspector,
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
const agentCall = WorkflowStepCall.make({
  stepId: WorkflowStepId.make("agent-step"),
  capability: WorkflowCapability.make("agent"),
  input: { prompt: "Check the build." },
});
const jobCall = WorkflowStepCall.make({
  stepId: WorkflowStepId.make("job-step"),
  capability: WorkflowCapability.make("job"),
  input: {
    command: ": > cwd-marker; printf 'job-finished\\n'",
  },
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

const capabilityLayer = <E, R>(
  filename: string,
  workspaceRoot: WorkspaceRoot,
  inspector: Layer.Layer<ProcessInspector, E, R>,
) => {
  const database = layerLoomSqlite({ filename });
  const agents = layerSqliteWorkflowChildAgentStore.pipe(Layer.provide(database));
  const jobs = layerSqliteWorkflowJobStore.pipe(Layer.provide(database));
  const processes = layerSqliteJobProcessStore.pipe(Layer.provide(database));
  const reconciler = layerJobReconciler.pipe(
    Layer.provide(Layer.mergeAll(processes, inspector, jobs)),
  );
  const capabilities = layerWorkflowCapabilities({ workspaceRoot }).pipe(
    Layer.provide([agents, jobs, processes, inspector, reconciler]),
  );
  return Layer.mergeAll(database, agents, jobs, processes, capabilities);
};

it.scopedLive.layer(BunServices.layer)(
  "returns stable Agent and background Job handles without duplicate work",
  () =>
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
        expect(yield* agents.listActiveBySession(agentContext.sessionId)).toHaveLength(1);
        expect(jobResults[0].value).toEqual(jobResults[1].value);

        const artifactWrite = WorkflowArtifactWrite.make({
          stepId: WorkflowStepId.make("artifact-step"),
          value: { result: "complete" },
        });
        const artifactResults = yield* Effect.all([
          artifacts.store(artifactWrite, agentContext),
          artifacts.store(artifactWrite, agentContext),
        ]);
        expect(artifactResults[0]).toEqual(artifactResults[1]);

        const job = yield* Schema.decodeUnknownEffect(WorkflowJobHandle)(jobResults[0].value);
        const stdoutPath = `${directory}/.loom/jobs/${encodeURIComponent(job.jobId)}/stdout.log`;
        expect(yield* waitForOutput(fs, stdoutPath, "job-finished\n")).toBe("job-finished\n");
        expect(yield* fs.exists(`${directory}/cwd-marker`)).toBe(true);

        yield* executor.compensate(agentCall, agentContext);
        yield* executor.compensate(agentCall, agentContext);
        yield* agents.stopSession(agentContext.sessionId);
        yield* agents.stopSession(agentContext.sessionId);
        expect(yield* agents.listActiveBySession(agentContext.sessionId)).toEqual([]);
      }).pipe(
        Effect.provide(
          capabilityLayer(`${directory}/loom.sqlite`, workspaceRoot, layerBunProcessInspector),
        ),
      );
    }),
);

it.scopedLive.layer(BunServices.layer)(
  "stops a child when launch setup fails and permits a durable retry",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-capability-retry-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const attemptedPid = yield* Ref.make(Option.none<number>());
      const failingInspector = Layer.succeed(
        ProcessInspector,
        ProcessInspector.of({
          inspect: (pid) =>
            Ref.set(attemptedPid, Option.some(pid)).pipe(
              Effect.andThen(
                Effect.fail(
                  new ProcessInspectionError({ pid, cause: "injected inspection failure" }),
                ),
              ),
            ),
        }),
      );

      const filename = `${directory}/loom.sqlite`;

      yield* Effect.gen(function* () {
        const executor = yield* WorkflowCapabilityExecutor;
        yield* executor.execute(jobCall, jobContext).pipe(Effect.flip);
      }).pipe(Effect.provide(capabilityLayer(filename, workspaceRoot, failingInspector)));

      const pid = yield* Ref.get(attemptedPid).pipe(
        Effect.flatMap(
          Option.match({
            onNone: () => Effect.die("The failing inspector did not receive a process ID."),
            onSome: Effect.succeed,
          }),
        ),
      );
      const liveInspector = yield* makeBunProcessInspector;
      const observation = yield* liveInspector.inspect(pid);
      expect(ProcessObservation.$is("Missing")(observation)).toBe(true);

      yield* Effect.gen(function* () {
        const executor = yield* WorkflowCapabilityExecutor;
        const result = yield* executor.execute(jobCall, jobContext);
        const job = yield* Schema.decodeUnknownEffect(WorkflowJobHandle)(result.value);
        const stdoutPath = `${directory}/.loom/jobs/${encodeURIComponent(job.jobId)}/stdout.log`;

        expect(yield* waitForOutput(fs, stdoutPath, "job-finished\n")).toBe("job-finished\n");
        expect(yield* fs.exists(`${directory}/cwd-marker`)).toBe(true);
      }).pipe(Effect.provide(capabilityLayer(filename, workspaceRoot, layerBunProcessInspector)));
    }),
);

it.scopedLive.layer(BunServices.layer)(
  "stops a gated launch on interruption and runs it once after retry",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "loom-capability-interrupt-",
      });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const attemptedPid = yield* Deferred.make<number>();
      const blockingInspector = Layer.succeed(
        ProcessInspector,
        ProcessInspector.of({
          inspect: (pid) => Deferred.succeed(attemptedPid, pid).pipe(Effect.andThen(Effect.never)),
        }),
      );
      const filename = `${directory}/loom.sqlite`;
      const sideEffectPath = `${directory}/side-effect`;
      const interruptedCall = WorkflowStepCall.make({
        ...jobCall,
        input: { command: `printf x >> '${sideEffectPath}'` },
      });

      const pid = yield* Effect.gen(function* () {
        const executor = yield* WorkflowCapabilityExecutor;
        const launch = yield* executor.execute(interruptedCall, jobContext).pipe(Effect.forkChild);
        const observedPid = yield* Deferred.await(attemptedPid);
        yield* Fiber.interrupt(launch);
        return observedPid;
      }).pipe(Effect.provide(capabilityLayer(filename, workspaceRoot, blockingInspector)));

      const inspector = yield* makeBunProcessInspector;
      expect(ProcessObservation.$is("Missing")(yield* inspector.inspect(pid))).toBe(true);

      yield* Effect.gen(function* () {
        const executor = yield* WorkflowCapabilityExecutor;
        yield* executor.execute(interruptedCall, jobContext);
        expect(yield* waitForOutput(fs, sideEffectPath, "x")).toBe("x");
      }).pipe(Effect.provide(capabilityLayer(filename, workspaceRoot, layerBunProcessInspector)));
    }),
);
