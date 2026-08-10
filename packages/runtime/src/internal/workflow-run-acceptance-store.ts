import type {
  WorkflowIdentity,
  WorkflowRequestDigest,
  WorkflowRunAddress,
  WorkflowRunId,
} from "@cvr/loom-domain";
import { Context, type Effect, type Option } from "effect";
import type { WorkflowRunAcceptanceError } from "@cvr/loom-protocol";

export interface WorkflowRunAcceptanceStoreShape {
  readonly claim: (
    identity: WorkflowIdentity,
    digest: WorkflowRequestDigest,
    workflowRunId: WorkflowRunId,
  ) => Effect.Effect<WorkflowRequestDigest, WorkflowRunAcceptanceError>;
  readonly lookup: (
    workflowRunId: WorkflowRunId,
  ) => Effect.Effect<Option.Option<WorkflowIdentity>, WorkflowRunAcceptanceError>;
  readonly list: Effect.Effect<ReadonlyArray<WorkflowRunAddress>, WorkflowRunAcceptanceError>;
}

export class WorkflowRunAcceptanceStore extends Context.Service<
  WorkflowRunAcceptanceStore,
  WorkflowRunAcceptanceStoreShape
>()("@cvr/loom-runtime/WorkflowRunAcceptanceStore") {}
