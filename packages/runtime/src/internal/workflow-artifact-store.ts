import { Context, type Effect } from "effect";
import type { WorkflowRunError } from "@cvr/loom-protocol";
import type {
  WorkflowArtifactReference,
  WorkflowArtifactWrite,
} from "./workflow-interpreter-model.js";

export interface WorkflowArtifactStoreShape {
  readonly store: (
    write: WorkflowArtifactWrite,
  ) => Effect.Effect<WorkflowArtifactReference, WorkflowRunError>;
}

export class WorkflowArtifactStore extends Context.Service<
  WorkflowArtifactStore,
  WorkflowArtifactStoreShape
>()("@cvr/loom-runtime/WorkflowArtifactStore") {}
