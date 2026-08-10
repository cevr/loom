import type { SessionId, WorkflowActivityKey, WorkflowChildAgent } from "@cvr/loom-domain";
import { Context, type Effect } from "effect";
import type { WorkflowActivityContext } from "./workflow-capability-model.js";
import type { WorkflowCapabilityStoreError } from "./workflow-capability-store-error.js";

export interface WorkflowChildAgentStoreShape {
  readonly claim: (
    context: WorkflowActivityContext,
    prompt: string,
  ) => Effect.Effect<WorkflowChildAgent, WorkflowCapabilityStoreError>;
  readonly stop: (
    activityKey: WorkflowActivityKey,
  ) => Effect.Effect<void, WorkflowCapabilityStoreError>;
  readonly listActiveBySession: (
    sessionId: SessionId,
  ) => Effect.Effect<ReadonlyArray<WorkflowChildAgent>, WorkflowCapabilityStoreError>;
  readonly stopSession: (sessionId: SessionId) => Effect.Effect<void, WorkflowCapabilityStoreError>;
}

export class WorkflowChildAgentStore extends Context.Service<
  WorkflowChildAgentStore,
  WorkflowChildAgentStoreShape
>()("@cvr/loom-runtime/WorkflowChildAgentStore") {}
