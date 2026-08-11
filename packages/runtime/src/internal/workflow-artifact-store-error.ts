import { ArtifactId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowArtifactStoreError extends Schema.TaggedError<WorkflowArtifactStoreError>()(
  "WorkflowArtifactStoreError",
  {
    artifactId: ArtifactId,
    cause: Schema.Defect(),
  },
) {}
