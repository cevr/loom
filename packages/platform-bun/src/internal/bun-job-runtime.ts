import {
  JobAddress,
  JobOutcome,
  JobRecord,
  JobSubmission,
  processIdentitiesMatch,
  type JobId,
  type JobRequest,
  type SessionId,
  type WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  ActorStateHub,
  JobRuntime,
  JobRuntimeError,
  JobStore,
  ProcessController,
  ProcessInspector,
  ProcessObservation,
  type JobRuntimeShape,
} from "@cvr/loom-runtime";
import { Duration, Effect, FiberMap, FileSystem, Layer, Option, Path, Scope } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import { detachJob, makeCancel, superviseCancellation } from "./bun-job-cancellation.js";
import {
  completeJob,
  jobAddress,
  type JobRuntimeServices,
  mapRuntimeError,
  publishJob,
  settleMissingJob,
} from "./bun-job-runtime-state.js";
import { makeAwait, makeReadOutput } from "./bun-job-io.js";
import { monitorJob, superviseJob } from "./bun-job-supervisor.js";

export interface BunJobRuntimeConfig {
  readonly workspaceRoot: WorkspaceRoot;
  readonly terminationGrace: Duration.Input;
}

const submissionFor = (workspaceRoot: WorkspaceRoot, request: JobRequest) => {
  const directory = `${workspaceRoot}/.loom/jobs/${encodeURIComponent(request.jobId)}`;
  return JobSubmission.make({
    ...request,
    stdoutPath: `${directory}/stdout.log`,
    stderrPath: `${directory}/stderr.log`,
    resultPath: `${directory}/result`,
  });
};

const sameSubmission = (job: JobRecord, submission: JobSubmission): boolean =>
  job.jobId === submission.jobId &&
  job.sessionId === submission.sessionId &&
  job.command === submission.command &&
  job.attached === submission.attached &&
  job.stdoutPath === submission.stdoutPath &&
  job.stderrPath === submission.stderrPath &&
  job.resultPath === submission.resultPath;

const acceptedRecord = (submission: JobSubmission) =>
  JobRecord.make({
    ...submission,
    status: "Accepted",
    identity: Option.none(),
    exitCode: Option.none(),
    detail: Option.none(),
  });

const makeStart = (
  services: JobRuntimeServices,
  config: BunJobRuntimeConfig,
  fibers: FiberMap.FiberMap<JobId>,
) =>
  Effect.fn("BunJobRuntime.start")((request: JobRequest) =>
    Effect.gen(function* () {
      const submission = submissionFor(config.workspaceRoot, request);
      const created = yield* services.jobs.create(submission).pipe(mapRuntimeError("start"));
      let job: JobRecord;
      if (created) {
        job = acceptedRecord(submission);
      } else {
        job = yield* services.jobs.get(JobAddress.make(request)).pipe(
          mapRuntimeError("start"),
          Effect.flatMap(
            Option.match({
              onNone: () =>
                Effect.fail(
                  new JobRuntimeError({ operation: "start", cause: "The Job record is missing." }),
                ),
              onSome: Effect.succeed,
            }),
          ),
        );
      }
      if (!sameSubmission(job, submission)) {
        return yield* new JobRuntimeError({
          operation: "start",
          cause: "The Job ID belongs to another request.",
        });
      }
      yield* publishJob(services.actors, job);
      if (job.status === "Accepted") {
        yield* superviseJob(services, config.workspaceRoot, config.terminationGrace, fibers, job);
      }
      return job;
    }).pipe(Effect.uninterruptible),
  );

const makeCloseSession = (services: JobRuntimeServices, cancel: JobRuntimeShape["cancel"]) =>
  Effect.fn("BunJobRuntime.closeSession")(function* (sessionId: SessionId) {
    const jobs = yield* services.jobs
      .listAttachedActive(sessionId)
      .pipe(mapRuntimeError("closeSession"));
    yield* Effect.forEach(jobs, (job) => cancel(jobAddress(job)), {
      concurrency: "unbounded",
      discard: true,
    });
  });

const reconcileUncommitted = (
  services: JobRuntimeServices,
  config: BunJobRuntimeConfig,
  fibers: FiberMap.FiberMap<JobId>,
  job: JobRecord,
) => {
  if (job.status === "Accepted") {
    return superviseJob(services, config.workspaceRoot, config.terminationGrace, fibers, job).pipe(
      Effect.as(job),
    );
  }
  return completeJob(
    services,
    job,
    JobOutcome.cases.Failed.make({
      exitCode: Option.none(),
      detail: Option.some("The Job launch did not commit before restart."),
    }),
  ).pipe(Effect.map(Option.getOrElse(() => job)));
};

const reconcileIdentity = (
  services: JobRuntimeServices,
  grace: Duration.Input,
  fibers: FiberMap.FiberMap<JobId>,
  cancellationFibers: FiberMap.FiberMap<JobId>,
  job: JobRecord,
) =>
  Option.match(job.identity, {
    onNone: () =>
      completeJob(
        services,
        job,
        JobOutcome.cases.Lost.make({
          detail: Option.some("The recoverable Job has no process identity."),
        }),
      ).pipe(Effect.map(Option.getOrElse(() => job))),
    onSome: (identity) =>
      services.inspector.inspect(identity.pid).pipe(
        mapRuntimeError("reconcile"),
        Effect.flatMap((observation) =>
          ProcessObservation.$match(observation, {
            Missing: () => {
              if (job.status === "Stopping") {
                return superviseCancellation(services, grace, cancellationFibers, job).pipe(
                  Effect.as(job),
                );
              }
              return settleMissingJob(services, job).pipe(Effect.map(Option.getOrElse(() => job)));
            },
            Found: ({ identity: actual }) => {
              if (processIdentitiesMatch(identity, actual)) {
                if (job.status === "Stopping") {
                  return superviseCancellation(services, grace, cancellationFibers, job).pipe(
                    Effect.as(job),
                  );
                }
                return monitorJob(services, fibers, job, identity).pipe(Effect.as(job));
              }
              return completeJob(
                services,
                job,
                JobOutcome.cases.Lost.make({
                  detail: Option.some("The Job process identity changed during restart."),
                }),
              ).pipe(Effect.map(Option.getOrElse(() => job)));
            },
          }),
        ),
      ),
  });

const makeReconcile = (
  services: JobRuntimeServices,
  config: BunJobRuntimeConfig,
  fibers: FiberMap.FiberMap<JobId>,
  cancellationFibers: FiberMap.FiberMap<JobId>,
) =>
  Effect.gen(function* () {
    const isolate = (effect: Effect.Effect<JobRecord, JobRuntimeError>, job: JobRecord) =>
      effect.pipe(
        Effect.catch((error) =>
          Effect.logWarning("Job reconciliation failed.", error).pipe(Effect.as(job)),
        ),
      );
    const uncommitted = yield* services.jobs.listUncommitted.pipe(mapRuntimeError("reconcile"));
    const first = yield* Effect.forEach(uncommitted, (job) =>
      isolate(reconcileUncommitted(services, config, fibers, job), job),
    );
    const recoverable = yield* services.jobs.listRecoverable.pipe(mapRuntimeError("reconcile"));
    const second = yield* Effect.forEach(recoverable, (job) =>
      isolate(
        reconcileIdentity(services, config.terminationGrace, fibers, cancellationFibers, job),
        job,
      ),
    );
    return [...first, ...second];
  });

export const makeBunJobRuntime = (
  config: BunJobRuntimeConfig,
): Effect.Effect<
  JobRuntimeShape,
  never,
  | ActorStateHub
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | JobStore
  | Path.Path
  | ProcessController
  | ProcessInspector
  | Scope.Scope
> =>
  Effect.gen(function* () {
    const services: JobRuntimeServices = {
      actors: yield* ActorStateHub,
      controller: yield* ProcessController,
      fs: yield* FileSystem.FileSystem,
      inspector: yield* ProcessInspector,
      jobs: yield* JobStore,
      path: yield* Path.Path,
      spawner: yield* ChildProcessSpawner.ChildProcessSpawner,
    };
    const fibers = yield* FiberMap.make<JobId>();
    const cancellationFibers = yield* FiberMap.make<JobId>();
    const start = makeStart(services, config, fibers);
    const cancel = makeCancel(services, config.terminationGrace, cancellationFibers);
    return JobRuntime.of({
      start,
      inspect: (address) => services.jobs.get(address).pipe(mapRuntimeError("inspect")),
      await: makeAwait(services),
      readOutput: makeReadOutput(services),
      cancel,
      detach: (address) => detachJob(services, address),
      closeSession: makeCloseSession(services, cancel),
      reconcile: makeReconcile(services, config, fibers, cancellationFibers),
    });
  });

export const layerBunJobRuntime = (
  config: BunJobRuntimeConfig,
): Layer.Layer<
  JobRuntime,
  never,
  | ActorStateHub
  | ChildProcessSpawner.ChildProcessSpawner
  | FileSystem.FileSystem
  | JobStore
  | Path.Path
  | ProcessController
  | ProcessInspector
> => Layer.effect(JobRuntime, makeBunJobRuntime(config));
