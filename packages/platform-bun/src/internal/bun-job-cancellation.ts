import { type JobAddress, type JobId, JobRecord } from "@cvr/loom-domain";
import { type Duration, Effect, FiberMap, Option } from "effect";
import { makeAwaitTerminal } from "./bun-job-io.js";
import { type JobRuntimeServices, mapRuntimeError, publishJob } from "./bun-job-runtime-state.js";
import { cancelRunningJob } from "./bun-job-supervisor.js";

const existingAfterMutation = (
  services: JobRuntimeServices,
  operation: "cancel" | "detach",
  address: JobAddress,
  updated: Option.Option<JobRecord>,
) => {
  if (Option.isSome(updated)) return Effect.succeed(updated);
  return services.jobs.get(address).pipe(mapRuntimeError(operation));
};

export const superviseCancellation = (
  services: JobRuntimeServices,
  grace: Duration.Input,
  fibers: FiberMap.FiberMap<JobId>,
  job: JobRecord,
) =>
  Option.match(job.identity, {
    onNone: () => Effect.void,
    onSome: (identity) =>
      FiberMap.run(
        fibers,
        job.jobId,
        cancelRunningJob(services, grace, job, identity).pipe(
          Effect.catchCause((cause) => Effect.logError("Job cancellation failed.", cause)),
          Effect.asVoid,
        ),
        { onlyIfMissing: true },
      ).pipe(Effect.asVoid),
  });

export const makeCancel = (
  services: JobRuntimeServices,
  grace: Duration.Input,
  fibers: FiberMap.FiberMap<JobId>,
) =>
  Effect.fn("BunJobRuntime.cancel")(function* (address: JobAddress) {
    const requestedJob = yield* Effect.gen(function* () {
      const requested = yield* services.jobs.requestStop(address).pipe(mapRuntimeError("cancel"));
      const current = yield* existingAfterMutation(services, "cancel", address, requested);
      if (Option.isSome(current) && Option.isSome(requested)) {
        yield* publishJob(services.actors, current.value);
        yield* superviseCancellation(services, grace, fibers, current.value);
      }
      return current;
    }).pipe(Effect.uninterruptible);
    if (Option.isNone(requestedJob)) return requestedJob;
    return yield* makeAwaitTerminal(services)(address);
  });

export const detachJob = (services: JobRuntimeServices, address: JobAddress) =>
  services.jobs.detach(address).pipe(
    mapRuntimeError("detach"),
    Effect.flatMap((updated) => existingAfterMutation(services, "detach", address, updated)),
  );
