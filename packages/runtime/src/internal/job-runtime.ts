import {
  type JobAddress,
  type JobRecord,
  type JobRequest,
  type JobTerminalRecord,
  type SessionId,
} from "@cvr/loom-domain";
import { Context, type Effect, type Option } from "effect";
import type { JobRuntimeError } from "./job-runtime-error.js";

export interface JobWaitRequest extends JobAddress {
  readonly foregroundLeaseMillis: number;
}

export type JobOutputStream = "stdout" | "stderr";

export interface JobOutputRequest extends JobAddress {
  readonly stream: JobOutputStream;
  readonly sequence: number;
  readonly maximumBytes: number;
}

export interface JobOutputChunk {
  readonly stream: JobOutputStream;
  readonly sequence: number;
  readonly nextSequence: number;
  readonly data: Uint8Array;
  readonly complete: boolean;
}

export interface JobRuntimeShape {
  readonly start: (request: JobRequest) => Effect.Effect<JobRecord, JobRuntimeError>;
  readonly inspect: (
    address: JobAddress,
  ) => Effect.Effect<Option.Option<JobRecord>, JobRuntimeError>;
  readonly await: (
    request: JobWaitRequest,
  ) => Effect.Effect<Option.Option<JobRecord>, JobRuntimeError>;
  readonly awaitTerminal: (
    address: JobAddress,
  ) => Effect.Effect<Option.Option<JobTerminalRecord>, JobRuntimeError>;
  readonly readOutput: (
    request: JobOutputRequest,
  ) => Effect.Effect<JobOutputChunk, JobRuntimeError>;
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
