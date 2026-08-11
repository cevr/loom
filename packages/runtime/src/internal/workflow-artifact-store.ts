import { Context, type Effect } from "effect";
import type { WorkflowRunError } from "@cvr/loom-protocol";
import type { WorkflowArtifactReference, WorkflowArtifactWrite } from "@cvr/loom-protocol";
import type { WorkflowActivityContext } from "./workflow-capability-model.js";

export interface WorkflowArtifactStoreShape {
  readonly store: (
    write: WorkflowArtifactWrite,
    context: WorkflowActivityContext,
  ) => Effect.Effect<WorkflowArtifactReference, WorkflowRunError>;
}

export class WorkflowArtifactStore extends Context.Service<
  WorkflowArtifactStore,
  WorkflowArtifactStoreShape
>()("@cvr/loom-runtime/WorkflowArtifactStore") {}
