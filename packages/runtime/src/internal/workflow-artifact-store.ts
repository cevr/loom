import type { WorkflowArtifactReference, WorkflowArtifactWrite } from "@cvr/loom-protocol";
import { Context, type Effect, Schema } from "effect";
import type { WorkflowArtifactNotFoundError } from "./workflow-artifact-not-found-error.js";
import type { WorkflowArtifactStoreError } from "./workflow-artifact-store-error.js";
import type { WorkflowActivityContext } from "./workflow-capability-model.js";

export interface WorkflowArtifactStoreShape {
  readonly store: (
    write: WorkflowArtifactWrite,
    context: WorkflowActivityContext,
  ) => Effect.Effect<WorkflowArtifactReference, WorkflowArtifactStoreError>;
  readonly read: (
    reference: WorkflowArtifactReference,
  ) => Effect.Effect<Schema.Json, WorkflowArtifactNotFoundError | WorkflowArtifactStoreError>;
}

export class WorkflowArtifactStore extends Context.Service<
  WorkflowArtifactStore,
  WorkflowArtifactStoreShape
>()("@cvr/loom-runtime/WorkflowArtifactStore") {}
