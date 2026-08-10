import {
  type JobAddress,
  type JobActiveStatus,
  type JobId,
  type JobOutcome,
  type JobRecord,
  type JobSubmission,
  type ProcessIdentity,
  type SessionId,
} from "@cvr/loom-domain";
import { Context, type Effect, type Option } from "effect";
import type { NonEmptyReadonlyArray } from "effect/Array";
import type { JobStoreError } from "./job-store-error.js";

export interface JobStoreShape {
  readonly create: (job: JobSubmission) => Effect.Effect<boolean, JobStoreError>;
  readonly get: (address: JobAddress) => Effect.Effect<Option.Option<JobRecord>, JobStoreError>;
  readonly begin: (jobId: JobId) => Effect.Effect<boolean, JobStoreError>;
  readonly activate: (
    jobId: JobId,
    identity: ProcessIdentity,
  ) => Effect.Effect<Option.Option<JobRecord>, JobStoreError>;
  readonly requestStop: (
    address: JobAddress,
  ) => Effect.Effect<Option.Option<JobRecord>, JobStoreError>;
  readonly complete: (jobId: JobId, outcome: JobOutcome) => Effect.Effect<boolean, JobStoreError>;
  readonly detach: (address: JobAddress) => Effect.Effect<Option.Option<JobRecord>, JobStoreError>;
  readonly listRecoverable: Effect.Effect<ReadonlyArray<JobRecord>, JobStoreError>;
  readonly listUncommitted: Effect.Effect<ReadonlyArray<JobRecord>, JobStoreError>;
  readonly listByStatus: (
    statuses: NonEmptyReadonlyArray<JobActiveStatus>,
  ) => Effect.Effect<ReadonlyArray<JobRecord>, JobStoreError>;
  readonly listAttachedActive: (
    sessionId: SessionId,
  ) => Effect.Effect<ReadonlyArray<JobRecord>, JobStoreError>;
}

export class JobStore extends Context.Service<JobStore, JobStoreShape>()(
  "@cvr/loom-runtime/JobStore",
) {}
