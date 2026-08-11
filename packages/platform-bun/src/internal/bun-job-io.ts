import { JobAddress, JobRecord } from "@cvr/loom-domain";
import {
  JobRuntimeError,
  type JobOutputChunk,
  type JobOutputRequest,
  type JobWaitRequest,
} from "@cvr/loom-runtime";
import { Duration, Effect, Option, type PlatformError, Schedule, Stream } from "effect";
import { type JobRuntimeServices, mapRuntimeError } from "./bun-job-runtime-state.js";

const isTerminal = JobRecord.isAnyOf(["Succeeded", "Failed", "Cancelled", "Lost"]);

export const makeAwaitTerminal = (services: JobRuntimeServices) =>
  Effect.fn("BunJobRuntime.awaitTerminal")((address: JobAddress) =>
    services.jobs.get(address).pipe(
      mapRuntimeError("await"),
      Effect.repeat({
        until: Option.match({ onNone: () => true, onSome: isTerminal }),
        schedule: Schedule.spaced("50 millis"),
      }),
    ),
  );

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
      isTerminal(job.value) &&
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
