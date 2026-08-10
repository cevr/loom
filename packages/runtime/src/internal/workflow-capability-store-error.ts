import { Data } from "effect";

export class WorkflowCapabilityStoreError extends Data.TaggedError("WorkflowCapabilityStoreError")<{
  readonly operation: string;
  readonly message: string;
}> {}
