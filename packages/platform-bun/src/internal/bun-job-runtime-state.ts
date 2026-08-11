import {
  ActorActivity,
  ActorStateProjection,
  ActorSubject,
  JobActiveStatus,
  JobAddress,
  JobFailure,
  JobOutcome,
  JobRecord,
  type JobId,
} from "@cvr/loom-domain";
import {
  JobRuntimeError,
  type ActorStateHubShape,
  type JobStoreShape,
  type ProcessControllerShape,
  type ProcessInspectorShape,
} from "@cvr/loom-runtime";
import { Deferred, Effect, FileSystem, Option, Path } from "effect";
import type { ChildProcessSpawner } from "effect/unstable/process";
import { readJobOutcome } from "./bun-job-command.js";

export interface JobRuntimeServices {
  readonly actors: ActorStateHubShape;
  readonly completions: Map<JobId, Deferred.Deferred<JobRecord>>;
  readonly controller: ProcessControllerShape;
  readonly fs: FileSystem.FileSystem;
  readonly inspector: ProcessInspectorShape;
  readonly jobs: JobStoreShape;
  readonly path: Path.Path;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
}

export const mapRuntimeError =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, JobRuntimeError, R> =>
    Effect.mapError(effect, (cause) => new JobRuntimeError({ operation, cause }));

export const jobAddress = (job: JobRecord) =>
  JobAddress.make({ jobId: job.jobId, sessionId: job.sessionId });

const revision = (job: JobRecord): number => {
  switch (job.status) {
    case "Accepted":
      return 0;
    case "Starting":
      return 1;
    case "Running":
      return 2;
    case "Stopping":
      return 3;
    case "Succeeded":
    case "Failed":
    case "Cancelled":
    case "Lost":
      return 4;
  }
};

const activity = (job: JobRecord) => {
  if (JobRecord.isAnyOf(JobActiveStatus.literals)(job)) {
    return ActorActivity.cases.Working.make({ message: `Job ${job.status}` });
  }
  if (job.status === "Failed") {
    return ActorActivity.cases.Failed.make({
      message: JobFailure.match(job.failure, {
        Launch: (failure) => failure.detail,
        Exit: (failure) => Option.getOrElse(failure.detail, () => "Job Failed"),
        Runtime: (failure) => failure.detail,
      }),
    });
  }
  if (job.status === "Lost") {
    return ActorActivity.cases.Failed.make({
      message: Option.getOrElse(job.detail, () => "Job Lost"),
    });
  }
  return ActorActivity.cases.Stopped.make({});
};

export const publishJob = (actors: ActorStateHubShape, job: JobRecord) =>
  actors.publish(
    ActorStateProjection.make({
      subject: ActorSubject.cases.Job.make({ sessionId: job.sessionId, jobId: job.jobId }),
      activity: activity(job),
      revision: revision(job),
    }),
  );

export const isJobTerminal = JobRecord.isAnyOf(["Succeeded", "Failed", "Cancelled", "Lost"]);

export const jobCompletion = (services: JobRuntimeServices, jobId: JobId) =>
  Effect.sync(() => {
    const current = Option.fromNullishOr(services.completions.get(jobId));
    if (Option.isSome(current)) return current.value;
    const created = Deferred.makeUnsafe<JobRecord>();
    services.completions.set(jobId, created);
    return created;
  });

export const removeJobCompletion = (
  services: JobRuntimeServices,
  jobId: JobId,
  completion: Deferred.Deferred<JobRecord>,
) =>
  Effect.sync(() => {
    if (services.completions.get(jobId) === completion) services.completions.delete(jobId);
  });

const signalCompletion = (services: JobRuntimeServices, job: JobRecord) =>
  Effect.sync(() => Option.fromNullishOr(services.completions.get(job.jobId))).pipe(
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.void,
        onSome: (completion) =>
          Deferred.succeed(completion, job).pipe(
            Effect.andThen(removeJobCompletion(services, job.jobId, completion)),
          ),
      }),
    ),
  );

export const completeJob = Effect.fn("BunJobRuntime.complete")(function* (
  services: JobRuntimeServices,
  job: JobRecord,
  outcome: JobOutcome,
) {
  yield* services.jobs.complete(job.jobId, outcome).pipe(mapRuntimeError("complete"));
  const current = yield* services.jobs.get(jobAddress(job)).pipe(mapRuntimeError("inspect"));
  yield* Option.match(current, {
    onNone: () => Effect.void,
    onSome: (record) =>
      Effect.gen(function* () {
        yield* publishJob(services.actors, record);
        if (isJobTerminal(record)) {
          yield* signalCompletion(services, record);
        }
      }),
  });
  return current;
});

export const settleMissingJob = Effect.fn("BunJobRuntime.settleMissing")(function* (
  services: JobRuntimeServices,
  observed: JobRecord,
) {
  const current = yield* services.jobs.get(jobAddress(observed)).pipe(mapRuntimeError("inspect"));
  if (Option.isNone(current)) return current;
  if (current.value.status === "Stopping") {
    return yield* completeJob(services, current.value, JobOutcome.cases.Cancelled.make({}));
  }
  const persisted = yield* readJobOutcome(services.fs, current.value).pipe(
    mapRuntimeError("readResult"),
  );
  const outcome = Option.getOrElse(persisted, () =>
    JobOutcome.cases.Lost.make({
      detail: Option.some("The Job process ended without a durable result."),
    }),
  );
  return yield* completeJob(services, current.value, outcome);
});

export const recordSupervisorFailure = Effect.fn("BunJobRuntime.recordSupervisorFailure")(
  function* (services: JobRuntimeServices, job: JobRecord, failure: JobFailure) {
    const current = yield* services.jobs.get(jobAddress(job)).pipe(mapRuntimeError("inspect"));
    if (Option.isNone(current)) return;
    switch (current.value.status) {
      case "Starting":
        yield* completeJob(
          services,
          current.value,
          JobOutcome.cases.Failed.make({
            failure,
          }),
        );
        return;
      case "Running":
        yield* completeJob(
          services,
          current.value,
          JobOutcome.cases.Failed.make({
            failure: JobFailure.cases.Runtime.make({
              detail: JobFailure.match(failure, {
                Launch: ({ detail }) => detail,
                Exit: ({ detail }) => Option.getOrElse(detail, () => "The Job process failed."),
                Runtime: ({ detail }) => detail,
              }),
            }),
          }),
        );
        return;
      case "Stopping":
        yield* completeJob(services, current.value, JobOutcome.cases.Cancelled.make({}));
        break;
      case "Accepted":
      case "Succeeded":
      case "Failed":
      case "Cancelled":
      case "Lost":
        break;
    }
  },
);
