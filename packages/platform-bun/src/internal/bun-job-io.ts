import { JobAddress } from "@cvr/loom-domain";
import {
  JobRuntimeError,
  type JobOutputChunk,
  type JobOutputRequest,
  type JobWaitRequest,
} from "@cvr/loom-runtime";
import { Deferred, Duration, Effect, Option, type PlatformError, Stream } from "effect";
import {
  isJobTerminal,
  jobCompletion,
  type JobRuntimeServices,
  mapRuntimeError,
  removeJobCompletion,
} from "./bun-job-runtime-state.js";

export const makeAwaitTerminal = (services: JobRuntimeServices) =>
  Effect.fn("BunJobRuntime.awaitTerminal")(function* (address: JobAddress) {
    const completion = yield* jobCompletion(services, address.jobId);
    const job = yield* services.jobs.get(address).pipe(mapRuntimeError("await"));
    if (Option.isNone(job)) {
      yield* removeJobCompletion(services, address.jobId, completion);
      return Option.none();
    }
    if (isJobTerminal(job.value)) {
      const terminal = job.value;
      yield* Deferred.succeed(completion, terminal);
      yield* removeJobCompletion(services, address.jobId, completion);
      return Option.some(terminal);
    }
    return Option.some(yield* Deferred.await(completion));
  });

export const makeAwait = (services: JobRuntimeServices) =>
  Effect.fn("BunJobRuntime.await")(function* (request: JobWaitRequest) {
    const inspect = services.jobs.get(JobAddress.make(request)).pipe(mapRuntimeError("await"));
    if (request.foregroundLeaseMillis === 0) return yield* inspect;
    return yield* makeAwaitTerminal(services)(JobAddress.make(request)).pipe(
      Effect.timeoutOrElse({
        duration: Duration.millis(request.foregroundLeaseMillis),
        orElse: () => inspect,
      }),
    );
  });

const ignoreMissing = <A>(effect: Effect.Effect<A, PlatformError.PlatformError>) =>
  effect.pipe(
    Effect.map(Option.some),
    Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(Option.none())),
  );

export const makeReadOutput = (services: JobRuntimeServices) =>
  Effect.fn("BunJobRuntime.readOutput")(function* (request: JobOutputRequest) {
    const job = yield* services.jobs
      .get(JobAddress.make(request))
      .pipe(mapRuntimeError("readOutput"));
    if (Option.isNone(job)) {
      return yield* new JobRuntimeError({
        operation: "readOutput",
        cause: "The Job record is missing.",
      });
    }
    let path = job.value.stdoutPath;
    if (request.stream === "stderr") path = job.value.stderrPath;
    const bytes = yield* services.fs
      .stream(path, { offset: request.sequence, bytesToRead: request.maximumBytes })
      .pipe(
        Stream.runCollect,
        Effect.map((chunks) => Buffer.concat(chunks)),
        ignoreMissing,
        mapRuntimeError("readOutput"),
      );
    const data = Option.getOrElse(bytes, () => new Uint8Array());
    const nextSequence = request.sequence + data.length;
    const info = yield* ignoreMissing(services.fs.stat(path)).pipe(mapRuntimeError("readOutput"));
    const complete =
      isJobTerminal(job.value) &&
      Option.match(info, {
        onNone: () => true,
        onSome: (file) => BigInt(nextSequence) >= file.size,
      });
    return {
      stream: request.stream,
      sequence: request.sequence,
      nextSequence,
      data,
      complete,
    } satisfies JobOutputChunk;
  });
