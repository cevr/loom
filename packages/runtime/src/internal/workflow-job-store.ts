import type { WorkflowActivityKey, WorkflowJob } from "@cvr/loom-domain";
import { Context, type Effect } from "effect";
import type { WorkflowActivityContext } from "./workflow-capability-model.js";
import type { WorkflowCapabilityStoreError } from "./workflow-capability-store-error.js";

export interface WorkflowJobStoreShape {
  readonly claim: (
    context: WorkflowActivityContext,
    attached: boolean,
  ) => Effect.Effect<WorkflowJob, WorkflowCapabilityStoreError>;
  readonly begin: (
    activityKey: WorkflowActivityKey,
  ) => Effect.Effect<boolean, WorkflowCapabilityStoreError>;
  readonly markRunning: (
    activityKey: WorkflowActivityKey,
  ) => Effect.Effect<void, WorkflowCapabilityStoreError>;
  readonly markFailed: (
    activityKey: WorkflowActivityKey,
  ) => Effect.Effect<void, WorkflowCapabilityStoreError>;
}

export class WorkflowJobStore extends Context.Service<WorkflowJobStore, WorkflowJobStoreShape>()(
  "@cvr/loom-runtime/WorkflowJobStore",
) {}
