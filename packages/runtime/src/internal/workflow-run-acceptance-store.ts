import {
  WorkflowIncarnationId,
  WorkflowIdentity,
  WorkflowRequestDigest,
  WorkflowRunId,
  type WorkflowRunAddress,
} from "@cvr/loom-domain";
import { Context, type Effect, type Option, Schema } from "effect";
import type { WorkflowRunAcceptanceError } from "@cvr/loom-protocol";

export const WorkflowRunClaim = Schema.TaggedUnion({
  Claimed: {
    incarnationId: WorkflowIncarnationId,
    workflowRunId: WorkflowRunId,
    digest: WorkflowRequestDigest,
  },
  Retiring: { workflowRunId: WorkflowRunId },
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
  readonly markRetiring: (
    address: WorkflowRunAddress,
  ) => Effect.Effect<void, WorkflowRunAcceptanceError>;
  readonly listActive: Effect.Effect<ReadonlyArray<WorkflowRunAddress>, WorkflowRunAcceptanceError>;
  readonly listRetiring: Effect.Effect<
    ReadonlyArray<WorkflowRunAddress>,
    WorkflowRunAcceptanceError
  >;
}

export class WorkflowRunAcceptanceStore extends Context.Service<
  WorkflowRunAcceptanceStore,
  WorkflowRunAcceptanceStoreShape
>()("@cvr/loom-runtime/WorkflowRunAcceptanceStore") {}
