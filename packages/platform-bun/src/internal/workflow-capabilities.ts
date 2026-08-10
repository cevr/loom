import {
  JobProcessRecord,
  WorkflowCapability,
  workflowArtifactId,
  type WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  ProcessInspector,
  ProcessObservation,
  JobProcessStore,
  WorkflowAgentHandle,
  WorkflowAgentInput,
  WorkflowArtifactReference,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  WorkflowChildAgentStore,
  WorkflowJobHandle,
  WorkflowJobInput,
  WorkflowJobStore,
  WorkflowStepExecution,
  type WorkflowActivityContext,
  type WorkflowArtifactStoreShape,
  type JobProcessStoreShape,
  type ProcessInspectorShape,
  type WorkflowChildAgentStoreShape,
  type WorkflowJobStoreShape,
  type WorkflowStepCall,
} from "@cvr/loom-runtime";
import { WorkflowStepError } from "@cvr/loom-protocol";
import { Effect, Exit, FileSystem, Inspectable, Layer, Option, Schema, Scope } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

const agentCapability = WorkflowCapability.make("agent");
const jobCapability = WorkflowCapability.make("job");
const supportedCapabilities = new Set([agentCapability, jobCapability]);
const decodeAgentInput = Schema.decodeUnknownEffect(WorkflowAgentInput);
const decodeJobInput = Schema.decodeUnknownEffect(WorkflowJobInput);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));

const stepError = (call: WorkflowStepCall, message: string) =>
  new WorkflowStepError({
    stepId: call.stepId,
    capability: call.capability,
    message,
  });

const decodeInput = <A>(
  call: WorkflowStepCall,
  decode: (input: Schema.Json) => Effect.Effect<A, object>,
) =>
  decode(call.input).pipe(
    Effect.mapError((cause) => stepError(call, Inspectable.toStringUnknown(cause))),
  );

const launchAgent = Effect.fn("WorkflowCapabilities.launchAgent")(function* (
  store: WorkflowChildAgentStoreShape,
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
) {
  const input = yield* decodeInput(call, decodeAgentInput);
  const agent = yield* store
    .claim(context, input.prompt)
    .pipe(Effect.mapError((error) => stepError(call, error.message)));
  return WorkflowStepExecution.make({
    value: WorkflowAgentHandle.make({ agentId: agent.agentId }),
    tokenCount: 0,
    agentRuns: 1,
  });
});

const makeJobCommand = (command: string, stdoutPath: string, stderrPath: string) =>
  ChildProcess.make(
    "/bin/sh",
    ["-lc", 'exec >"$1" 2>"$2"; eval "$3"', "loom-job", stdoutPath, stderrPath, command],
    {
      detached: true,
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
      killSignal: "SIGTERM",
      forceKillAfter: "500 millis",
    },
  );

const requireProcessIdentity = (call: WorkflowStepCall, observation: ProcessObservation) =>
  ProcessObservation.$match(observation, {
    Found: ({ identity }) => Effect.succeed(identity),
    Missing: ({ pid }) => Effect.fail(stepError(call, `Job process ${pid} disappeared at launch.`)),
  });

const prepareJobFiles = Effect.fn("WorkflowCapabilities.prepareJobFiles")(function* (
  fs: FileSystem.FileSystem,
  call: WorkflowStepCall,
  workspaceRoot: WorkspaceRoot,
  jobId: JobProcessRecord["jobId"],
) {
  const directory = `${workspaceRoot}/.loom/jobs/${encodeURIComponent(jobId)}`;
  yield* fs
    .makeDirectory(directory, { recursive: true })
    .pipe(Effect.mapError((cause) => stepError(call, Inspectable.toStringUnknown(cause))));
  return {
    stdoutPath: `${directory}/stdout.log`,
    stderrPath: `${directory}/stderr.log`,
  };
});

const observeJobExit = (
  processes: JobProcessStoreShape,
  child: ChildProcessSpawner.ChildProcessHandle,
  jobId: JobProcessRecord["jobId"],
  processScope: Scope.Closeable,
) =>
  Effect.forkDetach(
    child.exitCode.pipe(
      Effect.exit,
      Effect.flatMap(() =>
        processes
          .updateRecovery(jobId, "Exited", Option.none())
          .pipe(
            Effect.catchCause((cause) => Effect.logError("Job exit state update failed.", cause)),
          ),
      ),
      Effect.ensuring(Scope.close(processScope, Exit.void)),
    ),
  ).pipe(Effect.asVoid);

interface WorkflowJobServices {
  readonly fs: FileSystem.FileSystem;
  readonly inspector: ProcessInspectorShape;
  readonly jobs: WorkflowJobStoreShape;
  readonly processes: JobProcessStoreShape;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
}

const launchProcess = Effect.fn("WorkflowCapabilities.launchProcess")(function* (
  services: WorkflowJobServices,
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
  command: string,
  jobId: JobProcessRecord["jobId"],
  workspaceRoot: WorkspaceRoot,
) {
  const { stdoutPath, stderrPath } = yield* prepareJobFiles(
    services.fs,
    call,
    workspaceRoot,
    jobId,
  );
  const processScope = yield* Scope.make();
  const child = yield* services.spawner.spawn(makeJobCommand(command, stdoutPath, stderrPath)).pipe(
    Effect.provideService(Scope.Scope, processScope),
    Effect.mapError((cause) => stepError(call, Inspectable.toStringUnknown(cause))),
  );
  yield* Effect.gen(function* () {
    const identity = yield* services.inspector.inspect(child.pid).pipe(
      Effect.mapError((cause) => stepError(call, cause.message)),
      Effect.flatMap((observation) => requireProcessIdentity(call, observation)),
    );
    yield* services.jobs
      .markRunning(context.activityKey)
      .pipe(Effect.mapError((error) => stepError(call, error.message)));
    yield* services.processes
      .upsert(
        JobProcessRecord.make({
          jobId,
          sessionId: context.sessionId,
          identity,
          stdoutPath,
          stderrPath,
          status: "Running",
          recoveryDetail: Option.none(),
        }),
      )
      .pipe(Effect.mapError((error) => stepError(call, Inspectable.toStringUnknown(error.cause))));
    yield* child.unref.pipe(
      Effect.asVoid,
      Effect.mapError((cause) => stepError(call, Inspectable.toStringUnknown(cause))),
    );
    yield* observeJobExit(services.processes, child, jobId, processScope);
  }).pipe(Effect.onError(() => Scope.close(processScope, Exit.void)));
});

const launchJob = Effect.fn("WorkflowCapabilities.launchJob")(function* (
  services: WorkflowJobServices,
  workspaceRoot: WorkspaceRoot,
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
) {
  const input = yield* decodeInput(call, decodeJobInput);
  const job = yield* services.jobs
    .claim(context)
    .pipe(Effect.mapError((error) => stepError(call, error.message)));
  const ownsLaunch = yield* services.jobs
    .begin(context.activityKey)
    .pipe(Effect.mapError((error) => stepError(call, error.message)));
  if (ownsLaunch) {
    yield* launchProcess(services, call, context, input.command, job.jobId, workspaceRoot).pipe(
      Effect.tapError(() => services.jobs.markFailed(context.activityKey).pipe(Effect.orDie)),
    );
  }
  return WorkflowStepExecution.make({
    value: WorkflowJobHandle.make({ jobId: job.jobId }),
    tokenCount: 0,
    agentRuns: 0,
  });
});

const makeArtifactStore = (
  workspaceRoot: WorkspaceRoot,
): Effect.Effect<WorkflowArtifactStoreShape, never, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return WorkflowArtifactStore.of({
      store: (write, context) => {
        const artifactId = workflowArtifactId(context.activityKey);
        const path = `${workspaceRoot}/.loom/artifacts/${encodeURIComponent(artifactId)}.json`;
        return fs.makeDirectory(`${workspaceRoot}/.loom/artifacts`, { recursive: true }).pipe(
          Effect.andThen(encodeJson(write.value)),
          Effect.flatMap((value) => fs.writeFileString(path, value)),
          Effect.as(WorkflowArtifactReference.make({ artifactId })),
          Effect.mapError((cause) =>
            stepError(
              {
                stepId: write.stepId,
                capability: WorkflowCapability.make("artifact"),
                input: write.value,
              },
              Inspectable.toStringUnknown(cause),
            ),
          ),
        );
      },
    });
  });

export interface WorkflowCapabilitiesConfig {
  readonly workspaceRoot: WorkspaceRoot;
}

export const layerWorkflowCapabilities = (config: WorkflowCapabilitiesConfig) =>
  Layer.merge(
    Layer.effect(
      WorkflowCapabilityExecutor,
      Effect.gen(function* () {
        const childAgents = yield* WorkflowChildAgentStore;
        const jobs = yield* WorkflowJobStore;
        const processes = yield* JobProcessStore;
        const inspector = yield* ProcessInspector;
        const fs = yield* FileSystem.FileSystem;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        return WorkflowCapabilityExecutor.of({
          supports: (capability) => supportedCapabilities.has(capability),
          execute: (call, context) => {
            if (call.capability === agentCapability) {
              return launchAgent(childAgents, call, context);
            }
            if (call.capability === jobCapability) {
              return launchJob(
                { jobs, processes, inspector, fs, spawner },
                config.workspaceRoot,
                call,
                context,
              );
            }
            return Effect.fail(stepError(call, `No adapter is installed for ${call.capability}.`));
          },
          compensate: (call, context) => {
            if (call.capability !== agentCapability) return Effect.void;
            return childAgents
              .stop(context.activityKey)
              .pipe(Effect.mapError((error) => stepError(call, error.message)));
          },
        });
      }),
    ),
    Layer.effect(WorkflowArtifactStore, makeArtifactStore(config.workspaceRoot)),
  );
