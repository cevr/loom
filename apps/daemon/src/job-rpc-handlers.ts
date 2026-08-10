import { JobAddress, JobRequest, type JobId, type JobRecord } from "@cvr/loom-domain";
import {
  JobOutputChunk,
  type JobOperation,
  JobRpcError,
  JobState,
  type ReadJobOutputRequest,
  type StartJobRequest,
  type WaitForJobRequest,
} from "@cvr/loom-protocol";
import { type JobRuntimeError, type JobRuntimeShape } from "@cvr/loom-runtime";
import { Effect, Inspectable, Option } from "effect";

const failure = (jobId: JobId, operation: JobOperation, error: unknown) =>
  new JobRpcError({ jobId, operation, message: Inspectable.toStringUnknown(error) });

const requireJob = (
  jobId: JobId,
  operation: JobOperation,
  effect: Effect.Effect<Option.Option<JobRecord>, JobRuntimeError>,
) =>
  effect.pipe(
    Effect.mapError((error) => failure(jobId, operation, error)),
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.fail(failure(jobId, operation, "The Job does not exist.")),
        onSome: (job) => Effect.succeed(JobState.make(job)),
      }),
    ),
  );

export const makeJobRpcHandlers = (jobs: JobRuntimeShape) => ({
  "Job.Start": (request: StartJobRequest) =>
    jobs.start(JobRequest.make(request)).pipe(
      Effect.mapError((error) => failure(request.jobId, "start", error)),
      Effect.flatMap(() =>
        requireJob(
          request.jobId,
          "start",
          jobs.await({
            ...JobAddress.make(request),
            foregroundLeaseMillis: request.foregroundLeaseMillis,
          }),
        ),
      ),
    ),
  "Job.Inspect": (address: JobAddress) =>
    requireJob(address.jobId, "inspect", jobs.inspect(address)),
  "Job.Output": (request: ReadJobOutputRequest) =>
    jobs.readOutput(request).pipe(
      Effect.map(JobOutputChunk.make),
      Effect.mapError((error) => failure(request.jobId, "output", error)),
    ),
  "Job.Await": (request: WaitForJobRequest) =>
    requireJob(request.jobId, "await", jobs.await(request)),
  "Job.Cancel": (address: JobAddress) => requireJob(address.jobId, "cancel", jobs.cancel(address)),
  "Job.Detach": (address: JobAddress) => requireJob(address.jobId, "detach", jobs.detach(address)),
});
