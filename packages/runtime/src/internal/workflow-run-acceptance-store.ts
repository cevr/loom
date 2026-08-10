import type { WorkflowIdentity, WorkflowRequestDigest } from "@cvr/loom-domain";
import { Context, type Effect } from "effect";
import type { WorkflowRunAcceptanceError } from "@cvr/loom-protocol";

export interface WorkflowRunAcceptanceStoreShape {
  readonly claim: (
    identity: WorkflowIdentity,
    digest: WorkflowRequestDigest,
  ) => Effect.Effect<WorkflowRequestDigest, WorkflowRunAcceptanceError>;
}

export class WorkflowRunAcceptanceStore extends Context.Service<
  WorkflowRunAcceptanceStore,
  WorkflowRunAcceptanceStoreShape
>()("@cvr/loom-runtime/WorkflowRunAcceptanceStore") {}
