import { JobAddress, JobRecord, JobRequest } from "@cvr/loom-domain";
import { Effect, Schema } from "effect";
import { Rpc } from "effect/unstable/rpc";

const defaultForegroundLeaseMillis = 5 * 60 * 1_000;
const defaultJobOutputBytes = 16 * 1_024;
export const maximumJobOutputBytes = 256 * 1_024;

const foregroundLeaseMillis = Schema.Natural.pipe(
  Schema.withDecodingDefaultKey(Effect.succeed(defaultForegroundLeaseMillis)),
  Schema.withConstructorDefault(Effect.succeed(defaultForegroundLeaseMillis)),
);
const attached = Schema.Boolean.pipe(
  Schema.withDecodingDefaultKey(Effect.succeed(true)),
  Schema.withConstructorDefault(Effect.succeed(true)),
);
const outputSequence = Schema.Natural.pipe(
  Schema.withDecodingDefaultKey(Effect.succeed(0)),
  Schema.withConstructorDefault(Effect.succeed(0)),
);
const outputBytes = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: maximumJobOutputBytes }),
).pipe(
  Schema.withDecodingDefaultKey(Effect.succeed(defaultJobOutputBytes)),
  Schema.withConstructorDefault(Effect.succeed(defaultJobOutputBytes)),
);

export const JobState = Schema.Struct({
  jobId: JobRecord.fields.jobId,
  sessionId: JobRecord.fields.sessionId,
  command: JobRecord.fields.command,
  attached: JobRecord.fields.attached,
  status: JobRecord.fields.status,
  exitCode: JobRecord.fields.exitCode,
  detail: JobRecord.fields.detail,
});
export type JobState = typeof JobState.Type;

export const StartJobRequest = Schema.Struct({
  sessionId: JobRequest.fields.sessionId,
  jobId: JobRequest.fields.jobId,
  command: JobRequest.fields.command,
  attached,
  foregroundLeaseMillis,
});
export type StartJobRequest = typeof StartJobRequest.Type;

export const WaitForJobRequest = Schema.Struct({
  ...JobAddress.fields,
  foregroundLeaseMillis,
});
export type WaitForJobRequest = typeof WaitForJobRequest.Type;

export const JobOutputStream = Schema.Literals(["stdout", "stderr"]);
export type JobOutputStream = typeof JobOutputStream.Type;

export const ReadJobOutputRequest = Schema.Struct({
  ...JobAddress.fields,
  stream: JobOutputStream,
  sequence: outputSequence,
  maximumBytes: outputBytes,
});
export type ReadJobOutputRequest = typeof ReadJobOutputRequest.Type;

export const JobOutputChunk = Schema.Struct({
  stream: JobOutputStream,
  sequence: Schema.Natural,
  nextSequence: Schema.Natural,
  data: Schema.Uint8ArrayFromBase64,
  complete: Schema.Boolean,
});
export type JobOutputChunk = typeof JobOutputChunk.Type;

export const JobOperation = Schema.Literals([
  "start",
  "inspect",
  "output",
  "await",
  "cancel",
  "detach",
]);
export type JobOperation = typeof JobOperation.Type;

export class JobRpcError extends Schema.TaggedError<JobRpcError>()("JobRpcError", {
  jobId: JobRecord.fields.jobId,
  operation: JobOperation,
  message: Schema.String,
}) {}

export const StartJob = Rpc.make("Job.Start", {
  payload: StartJobRequest,
  success: JobState,
  error: JobRpcError,
});

export const InspectJob = Rpc.make("Job.Inspect", {
  payload: JobAddress,
  success: JobState,
  error: JobRpcError,
});

export const ReadJobOutput = Rpc.make("Job.Output", {
  payload: ReadJobOutputRequest,
  success: JobOutputChunk,
  error: JobRpcError,
});

export const WaitForJob = Rpc.make("Job.Await", {
  payload: WaitForJobRequest,
  success: JobState,
  error: JobRpcError,
});

export const CancelJob = Rpc.make("Job.Cancel", {
  payload: JobAddress,
  success: JobState,
  error: JobRpcError,
});

export const DetachJob = Rpc.make("Job.Detach", {
  payload: JobAddress,
  success: JobState,
  error: JobRpcError,
});
