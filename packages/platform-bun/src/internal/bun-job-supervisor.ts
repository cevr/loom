import {
  JobOutcome,
  type JobId,
  JobRecord,
  type ProcessIdentity,
  type WorkspaceRoot,
} from "@cvr/loom-domain";
import { JobRuntimeError, ProcessObservation } from "@cvr/loom-runtime";
import { Duration, Effect, Exit, FiberMap, Inspectable, Option, Schedule } from "effect";
import type { ChildProcessSpawner } from "effect/unstable/process";
import { makeJobCommand, outcomeForExitCode, releaseJob } from "./bun-job-command.js";
import {
  completeJob,
  identitiesMatch,
  jobAddress,
  type JobRuntimeServices,
  mapRuntimeError,
  publishJob,
  recordLaunchFailure,
  settleMissingJob,
} from "./bun-job-runtime-state.js";

const requireIdentity = (observation: ProcessObservation) =>
  ProcessObservation.$match(observation, {
    Missing: ({ pid }) =>
      Effect.fail(
        new JobRuntimeError({ operation: "launch", cause: `Process ${pid} disappeared.` }),
      ),
    Found: ({ identity }) => Effect.succeed(identity),
  });

const prepareProcess = Effect.fn("BunJobRuntime.prepareProcess")(function* (
  services: JobRuntimeServices,
  workspaceRoot: WorkspaceRoot,
  job: JobRecord,
) {
  yield* services.fs
    .makeDirectory(services.path.dirname(job.stdoutPath), { recursive: true })
    .pipe(
      Effect.andThen(
        Effect.all(
          [
            services.fs.writeFileString(job.stdoutPath, ""),
            services.fs.writeFileString(job.stderrPath, ""),
          ],
          { concurrency: 2, discard: true },
        ),
      ),
      mapRuntimeError("prepareFiles"),
    );
  const child = yield* services.spawner
    .spawn(makeJobCommand(job, workspaceRoot))
    .pipe(mapRuntimeError("spawn"));
  const identity = yield* services.inspector
    .inspect(child.pid)
    .pipe(mapRuntimeError("inspectProcess"), Effect.flatMap(requireIdentity));
  const active = yield* services.jobs
    .activate(job.jobId, identity)
    .pipe(mapRuntimeError("activate"));
  return { active, child, identity };
});

const outcomeForExit = (exit: Exit.Exit<ChildProcessSpawner.ExitCode, unknown>): JobOutcome =>
  Exit.match(exit, {
    onSuccess: outcomeForExitCode,
    onFailure: (cause) =>
      JobOutcome.cases.Failed.make({
        exitCode: Option.none(),
        detail: Option.some(Inspectable.toStringUnknown(cause)),
      }),
  });

const settleLiveExit = Effect.fn("BunJobRuntime.settleLiveExit")(function* (
  services: JobRuntimeServices,
  job: JobRecord,
  child: ChildProcessSpawner.ChildProcessHandle,
) {
  const exit = yield* Effect.exit(child.exitCode);
  const current = yield* services.jobs.get(jobAddress(job)).pipe(mapRuntimeError("inspect"));
  if (Option.isNone(current)) return;
  if (current.value.status === "Stopping") {
    yield* completeJob(services, current.value, JobOutcome.cases.Cancelled.make({}));
    return;
  }
  yield* completeJob(services, current.value, outcomeForExit(exit));
});

interface TerminationObservation {
  readonly groupAlive: boolean;
  readonly process: ProcessObservation;
}

const inspectTermination = (
  services: JobRuntimeServices,
  identity: ProcessIdentity,
): Effect.Effect<TerminationObservation, JobRuntimeError> =>
  Effect.all({
    groupAlive: services.controller.isGroupAlive(identity),
    process: services.inspector.inspect(identity.pid).pipe(mapRuntimeError("cancel")),
  });

const stillRunning = (identity: ProcessIdentity, observation: TerminationObservation): boolean =>
  ProcessObservation.$match(observation.process, {
    Missing: () => observation.groupAlive,
    Found: ({ identity: actual }) => identitiesMatch(identity, actual),
  });

const waitForTermination = (
  services: JobRuntimeServices,
  grace: Duration.Input,
  identity: ProcessIdentity,
) =>
  inspectTermination(services, identity).pipe(
    Effect.repeat({
      while: (observation) => stillRunning(identity, observation),
      schedule: Schedule.spaced("50 millis"),
    }),
    Effect.timeoutOrElse({
      duration: grace,
      orElse: () => inspectTermination(services, identity),
    }),
  );

const signalUnlessMissing = (
  services: JobRuntimeServices,
  identity: ProcessIdentity,
  signal: "SIGTERM" | "SIGKILL",
) =>
  services.controller.signalGroup(identity, signal).pipe(
    Effect.catch((error) =>
      services.controller.isGroupAlive(identity).pipe(
        Effect.flatMap((alive) => {
          if (alive) return Effect.fail(error);
          return Effect.void;
        }),
      ),
    ),
    mapRuntimeError("cancel"),
  );

export const cancelRunningJob = Effect.fn("BunJobRuntime.cancelRunning")(function* (
  services: JobRuntimeServices,
  grace: Duration.Input,
  job: JobRecord,
  identity: ProcessIdentity,
) {
  const before = yield* services.inspector.inspect(identity.pid).pipe(mapRuntimeError("cancel"));
  if (ProcessObservation.$is("Missing")(before)) return yield* settleMissingJob(services, job);
  if (!identitiesMatch(identity, before.identity)) {
    return yield* completeJob(
      services,
      job,
      JobOutcome.cases.Lost.make({
        detail: Option.some("The Job process identity changed before cancellation."),
      }),
    );
  }

  yield* signalUnlessMissing(services, identity, "SIGTERM");
  const after = yield* waitForTermination(services, grace, identity);
  if (ProcessObservation.$is("Found")(after.process)) {
    if (identitiesMatch(identity, after.process.identity)) {
      yield* signalUnlessMissing(services, identity, "SIGKILL");
      return yield* completeJob(services, job, JobOutcome.cases.Cancelled.make({}));
    }
    return yield* completeJob(
      services,
      job,
      JobOutcome.cases.Lost.make({
        detail: Option.some("The Job process identity changed during cancellation."),
      }),
    );
  }
  if (after.groupAlive) {
    yield* signalUnlessMissing(services, identity, "SIGKILL");
  }
  return yield* completeJob(services, job, JobOutcome.cases.Cancelled.make({}));
});

const launch = Effect.fn("BunJobRuntime.launch")(function* (
  services: JobRuntimeServices,
  workspaceRoot: WorkspaceRoot,
  grace: Duration.Input,
  job: JobRecord,
) {
  const ownsLaunch = yield* services.jobs.begin(job.jobId).pipe(mapRuntimeError("begin"));
  if (!ownsLaunch) return;
  yield* publishJob(services.actors, JobRecord.make({ ...job, status: "Starting" }));
  yield* Effect.scoped(
    Effect.gen(function* () {
      const prepared = yield* prepareProcess(services, workspaceRoot, job);
      if (Option.isNone(prepared.active)) return;
      yield* publishJob(services.actors, prepared.active.value);
      if (prepared.active.value.status === "Stopping") {
        yield* cancelRunningJob(services, grace, prepared.active.value, prepared.identity);
        return;
      }
      yield* prepared.child.unref.pipe(Effect.asVoid, mapRuntimeError("unref"));
      yield* releaseJob(prepared.child).pipe(mapRuntimeError("release"));
      yield* settleLiveExit(services, job, prepared.child);
    }),
  );
});

export const superviseJob = (
  services: JobRuntimeServices,
  workspaceRoot: WorkspaceRoot,
  grace: Duration.Input,
  fibers: FiberMap.FiberMap<JobId>,
  job: JobRecord,
) =>
  FiberMap.run(
    fibers,
    job.jobId,
    launch(services, workspaceRoot, grace, job).pipe(
      Effect.catch((error) =>
        recordLaunchFailure(services, job, Inspectable.toStringUnknown(error)).pipe(
          Effect.catchCause((completionCause) =>
            Effect.logError("Job launch and failure recording failed.", completionCause),
          ),
        ),
      ),
    ),
    { onlyIfMissing: true },
  ).pipe(Effect.asVoid);

export const monitorJob = (
  services: JobRuntimeServices,
  fibers: FiberMap.FiberMap<JobId>,
  job: JobRecord,
  identity: ProcessIdentity,
) => {
  const pass = services.inspector.inspect(identity.pid).pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        Effect.logWarning("Job process inspection failed.", error).pipe(Effect.as(true)),
      onSuccess: (observation) =>
        ProcessObservation.$match(observation, {
          Missing: () => settleMissingJob(services, job).pipe(Effect.as(false)),
          Found: ({ identity: actual }) => {
            if (identitiesMatch(identity, actual)) return Effect.succeed(true);
            return completeJob(
              services,
              job,
              JobOutcome.cases.Lost.make({
                detail: Option.some("The Job process identity changed after restart."),
              }),
            ).pipe(Effect.as(false));
          },
        }),
    }),
    Effect.catch((error) =>
      Effect.logWarning("Job recovery pass failed.", error).pipe(Effect.as(true)),
    ),
  );
  return FiberMap.run(
    fibers,
    job.jobId,
    pass.pipe(
      Effect.repeat({ while: (running) => running, schedule: Schedule.spaced("250 millis") }),
      Effect.asVoid,
    ),
    { onlyIfMissing: true },
  ).pipe(Effect.asVoid);
};
