import type { WorkflowRunAddress } from "@cvr/loom-domain";
import { Context, type Effect } from "effect";
import type { WorkflowRunRetentionError } from "./workflow-run-retention-error.js";

export interface WorkflowRunRetentionShape {
  readonly retire: (address: WorkflowRunAddress) => Effect.Effect<void, WorkflowRunRetentionError>;
}

export class WorkflowRunRetention extends Context.Service<
  WorkflowRunRetention,
  WorkflowRunRetentionShape
>()("@cvr/loom-runtime/WorkflowRunRetention") {}
