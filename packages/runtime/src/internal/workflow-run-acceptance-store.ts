import {
  WorkflowIncarnationId,
  WorkflowIdentity,
  WorkflowRequestDigest,
  WorkflowRunId,
  type WorkflowRunAddress,
} from "@cvr/loom-domain";
import { Context, type Effect, type Option, Schema } from "effect";
import type { WorkflowRunAcceptanceError } from "@cvr/loom-protocol";

export const WorkflowRunClaim = Schema.Struct({
  incarnationId: WorkflowIncarnationId,
  workflowRunId: WorkflowRunId,
  digest: WorkflowRequestDigest,
});
export type WorkflowRunClaim = typeof WorkflowRunClaim.Type;

export interface WorkflowRunAcceptanceStoreShape {
  readonly claim: (
    identity: WorkflowIdentity,
    digest: WorkflowRequestDigest,
    incarnationId: WorkflowIncarnationId,
    workflowRunId: WorkflowRunId,
  ) => Effect.Effect<WorkflowRunClaim, WorkflowRunAcceptanceError>;
  readonly lookup: (
    workflowRunId: WorkflowRunId,
  ) => Effect.Effect<Option.Option<WorkflowIdentity>, WorkflowRunAcceptanceError>;
  readonly list: Effect.Effect<ReadonlyArray<WorkflowRunAddress>, WorkflowRunAcceptanceError>;
}

export class WorkflowRunAcceptanceStore extends Context.Service<
  WorkflowRunAcceptanceStore,
  WorkflowRunAcceptanceStoreShape
>()("@cvr/loom-runtime/WorkflowRunAcceptanceStore") {}
