import {
  ActorActivity,
  ActorStateProjection,
  ActorSubject,
  JobActiveStatus,
  JobAddress,
  JobOutcome,
  type JobRecord,
  type ProcessIdentity,
} from "@cvr/loom-domain";
import {
  JobRuntimeError,
  type ActorStateHubShape,
  type JobStoreShape,
  type ProcessControllerShape,
  type ProcessInspectorShape,
} from "@cvr/loom-runtime";
import { Effect, FileSystem, Option, Path, Schema } from "effect";
import type { ChildProcessSpawner } from "effect/unstable/process";
import { readJobOutcome } from "./bun-job-command.js";

export interface JobRuntimeServices {
  readonly actors: ActorStateHubShape;
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

export const identitiesMatch = (expected: ProcessIdentity, actual: ProcessIdentity): boolean =>
  expected.pid === actual.pid &&
  expected.processGroupId === actual.processGroupId &&
  expected.processStartId === actual.processStartId;

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
    default:
      return 4;
  }
};

const isActiveStatus = Schema.is(JobActiveStatus);

const activity = (job: JobRecord) => {
  if (isActiveStatus(job.status)) {
    return ActorActivity.cases.Working.make({ message: `Job ${job.status}` });
  }
  if (job.status === "Failed" || job.status === "Lost") {
    return ActorActivity.cases.Failed.make({
      message: Option.getOrElse(job.detail, () => `Job ${job.status}`),
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

export const completeJob = Effect.fn("BunJobRuntime.complete")(function* (
  services: JobRuntimeServices,
  job: JobRecord,
  outcome: JobOutcome,
) {
  yield* services.jobs.complete(job.jobId, outcome).pipe(mapRuntimeError("complete"));
  const current = yield* services.jobs.get(jobAddress(job)).pipe(mapRuntimeError("inspect"));
  yield* Option.match(current, {
    onNone: () => Effect.void,
    onSome: (record) => publishJob(services.actors, record),
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

export const recordLaunchFailure = Effect.fn("BunJobRuntime.recordLaunchFailure")(function* (
  services: JobRuntimeServices,
  job: JobRecord,
  detail: string,
) {
  const current = yield* services.jobs.get(jobAddress(job)).pipe(mapRuntimeError("inspect"));
  if (Option.isNone(current)) return;
  let outcome: JobOutcome;
  if (current.value.status === "Stopping") {
    outcome = JobOutcome.cases.Cancelled.make({});
  } else {
    outcome = JobOutcome.cases.Failed.make({
      exitCode: Option.none(),
      detail: Option.some(detail),
    });
  }
  yield* completeJob(services, current.value, outcome);
});
