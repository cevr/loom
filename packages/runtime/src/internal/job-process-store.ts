import { type JobId, type JobProcessRecord, type JobProcessStatus } from "@cvr/loom-domain";
import { Context, type Effect } from "effect";
import type { JobProcessStoreError } from "./job-process-store-error.js";

export interface JobProcessStoreShape {
  readonly initialize: Effect.Effect<void, JobProcessStoreError>;
  readonly upsert: (record: JobProcessRecord) => Effect.Effect<void, JobProcessStoreError>;
  readonly listRecoverable: Effect.Effect<ReadonlyArray<JobProcessRecord>, JobProcessStoreError>;
  readonly updateRecovery: (
    jobId: JobId,
    status: JobProcessStatus,
    detail: string | null,
  ) => Effect.Effect<void, JobProcessStoreError>;
}

export class JobProcessStore extends Context.Service<JobProcessStore, JobProcessStoreShape>()(
  "@cvr/loom-runtime/JobProcessStore",
) {}
