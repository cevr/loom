import { type JobAddress, type JobRecord, type JobRequest, type SessionId } from "@cvr/loom-domain";
import { Context, type Effect, type Option } from "effect";
import type { JobRuntimeError } from "./job-runtime-error.js";

export interface JobRuntimeShape {
  readonly start: (request: JobRequest) => Effect.Effect<JobRecord, JobRuntimeError>;
  readonly inspect: (
    address: JobAddress,
  ) => Effect.Effect<Option.Option<JobRecord>, JobRuntimeError>;
  readonly cancel: (
    address: JobAddress,
  ) => Effect.Effect<Option.Option<JobRecord>, JobRuntimeError>;
  readonly detach: (
    address: JobAddress,
  ) => Effect.Effect<Option.Option<JobRecord>, JobRuntimeError>;
  readonly closeSession: (sessionId: SessionId) => Effect.Effect<void, JobRuntimeError>;
  readonly reconcile: Effect.Effect<ReadonlyArray<JobRecord>, JobRuntimeError>;
}

export class JobRuntime extends Context.Service<JobRuntime, JobRuntimeShape>()(
  "@cvr/loom-runtime/JobRuntime",
) {}
