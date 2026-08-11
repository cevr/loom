import type { WorkflowRunAddress } from "@cvr/loom-domain";
import { Context, type Duration, type Effect } from "effect";
import type { WorkflowRunRetentionError } from "./workflow-run-retention-error.js";

export interface WorkflowRunRetentionShape {
  readonly resumeRetirement: (
    address: WorkflowRunAddress,
  ) => Effect.Effect<void, WorkflowRunRetentionError>;
  readonly retireExpired: (
    address: WorkflowRunAddress,
    stateLease: Duration.Input,
  ) => Effect.Effect<void, WorkflowRunRetentionError>;
  readonly retireAfterLease: (
    address: WorkflowRunAddress,
    stateLease: Duration.Input,
  ) => Effect.Effect<void, WorkflowRunRetentionError>;
}

export class WorkflowRunRetention extends Context.Service<
  WorkflowRunRetention,
  WorkflowRunRetentionShape
>()("@cvr/loom-runtime/WorkflowRunRetention") {}
