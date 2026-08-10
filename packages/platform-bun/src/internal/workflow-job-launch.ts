import { JobProcessRecord, type WorkspaceRoot } from "@cvr/loom-domain";
import {
  ProcessObservation,
  type JobProcessStoreShape,
  type JobReconcilerShape,
  type ProcessInspectorShape,
  type WorkflowActivityContext,
  type WorkflowJobStoreShape,
  type WorkflowStepCall,
} from "@cvr/loom-runtime";
import { WorkflowStepError } from "@cvr/loom-protocol";
import { Effect, FileSystem, Inspectable, Option, Scope, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

const makeJobCommand = (
  command: string,
  stdoutPath: string,
  stderrPath: string,
  workspaceRoot: WorkspaceRoot,
) =>
  ChildProcess.make(
    "/bin/sh",
    [
      "-c",
      'IFS= read -r launch || exit 0; [ "$launch" = start ] || exit 0; exec </dev/null >"$1" 2>"$2"; eval "$3"',
      "loom-job",
      stdoutPath,
      stderrPath,
      command,
    ],
    {
      cwd: workspaceRoot,
      detached: true,
      stdin: { stream: "pipe" },
      stdout: "ignore",
      stderr: "ignore",
      killSignal: "SIGTERM",
      forceKillAfter: "500 millis",
    },
  );

const stepError = (call: WorkflowStepCall, message: string) =>
  new WorkflowStepError({ stepId: call.stepId, capability: call.capability, message });

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
  return { stdoutPath: `${directory}/stdout.log`, stderrPath: `${directory}/stderr.log` };
});

const releaseJobProcess = (child: ChildProcessSpawner.ChildProcessHandle, call: WorkflowStepCall) =>
  Stream.succeed("start\n").pipe(
    Stream.encodeText,
    Stream.run(child.stdin),
    Effect.mapError((cause) => stepError(call, Inspectable.toStringUnknown(cause))),
  );

export interface WorkflowJobServices {
  readonly fs: FileSystem.FileSystem;
  readonly inspector: ProcessInspectorShape;
  readonly jobs: WorkflowJobStoreShape;
  readonly processes: JobProcessStoreShape;
  readonly reconciler: JobReconcilerShape;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
}

type RestoreInterruptibility = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;

const registerJobProcess = Effect.fn("WorkflowCapabilities.registerJobProcess")(function* (
  services: WorkflowJobServices,
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
  child: ChildProcessSpawner.ChildProcessHandle,
  jobId: JobProcessRecord["jobId"],
  stdoutPath: string,
  stderrPath: string,
) {
  const identity = yield* services.inspector.inspect(child.pid).pipe(
    Effect.mapError((cause) => stepError(call, cause.message)),
    Effect.flatMap((observation) => requireProcessIdentity(call, observation)),
  );
  const record = JobProcessRecord.make({
    jobId,
    sessionId: context.sessionId,
    identity,
    stdoutPath,
    stderrPath,
    status: "Running",
    recoveryDetail: Option.none(),
  });
  yield* services.processes
    .upsert(record)
    .pipe(Effect.mapError((error) => stepError(call, Inspectable.toStringUnknown(error.cause))));
  return record;
});

export const launchProcess = Effect.fn("WorkflowCapabilities.launchProcess")(function* (
  services: WorkflowJobServices,
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
  command: string,
  jobId: JobProcessRecord["jobId"],
  workspaceRoot: WorkspaceRoot,
  restore: RestoreInterruptibility,
) {
  yield* Effect.scopedWith((processScope) =>
    Effect.gen(function* () {
      const { child, record } = yield* restore(
        Effect.gen(function* () {
          const { stdoutPath, stderrPath } = yield* prepareJobFiles(
            services.fs,
            call,
            workspaceRoot,
            jobId,
          );
          const spawnedChild = yield* services.spawner
            .spawn(makeJobCommand(command, stdoutPath, stderrPath, workspaceRoot))
            .pipe(
              Effect.provideService(Scope.Scope, processScope),
              Effect.mapError((cause) => stepError(call, Inspectable.toStringUnknown(cause))),
            );
          const durableRecord = yield* registerJobProcess(
            services,
            call,
            context,
            spawnedChild,
            jobId,
            stdoutPath,
            stderrPath,
          );
          return { child: spawnedChild, record: durableRecord };
        }),
      );
      yield* services.jobs
        .markRunning(context.activityKey)
        .pipe(Effect.mapError((error) => stepError(call, error.message)));
      yield* child.unref.pipe(
        Effect.asVoid,
        Effect.mapError((cause) => stepError(call, Inspectable.toStringUnknown(cause))),
      );
      yield* releaseJobProcess(child, call);
      yield* services.reconciler.watch(record);
    }),
  );
});
