import {
  type JobAddress,
  type JobAcceptedRecord,
  type JobId,
  type JobOutcome,
  type JobRecord,
  type JobRecoverableRecord,
  type JobSubmission,
  type JobStartingRecord,
  type JobUncommittedRecord,
  type ProcessIdentity,
  type SessionId,
} from "@cvr/loom-domain";
import { Context, type Effect, type Option } from "effect";
import type { JobStoreError } from "./job-store-error.js";

export interface JobStoreShape {
  readonly create: (
    job: JobSubmission,
  ) => Effect.Effect<Option.Option<JobAcceptedRecord>, JobStoreError>;
  readonly get: (address: JobAddress) => Effect.Effect<Option.Option<JobRecord>, JobStoreError>;
  readonly begin: (jobId: JobId) => Effect.Effect<Option.Option<JobStartingRecord>, JobStoreError>;
  readonly activate: (
    jobId: JobId,
    identity: ProcessIdentity,
  ) => Effect.Effect<Option.Option<JobRecord>, JobStoreError>;
  readonly requestStop: (
    address: JobAddress,
  ) => Effect.Effect<Option.Option<JobRecord>, JobStoreError>;
  readonly complete: (jobId: JobId, outcome: JobOutcome) => Effect.Effect<boolean, JobStoreError>;
  readonly detach: (address: JobAddress) => Effect.Effect<Option.Option<JobRecord>, JobStoreError>;
  readonly listRecoverable: Effect.Effect<ReadonlyArray<JobRecoverableRecord>, JobStoreError>;
  readonly listUncommitted: Effect.Effect<ReadonlyArray<JobUncommittedRecord>, JobStoreError>;
  readonly listAttachedActive: (
    sessionId: SessionId,
  ) => Effect.Effect<ReadonlyArray<JobRecord>, JobStoreError>;
}

export class JobStore extends Context.Service<JobStore, JobStoreShape>()(
  "@cvr/loom-runtime/JobStore",
) {}
