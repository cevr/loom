import { ArtifactId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowArtifactNotFoundError extends Schema.TaggedError<WorkflowArtifactNotFoundError>()(
  "WorkflowArtifactNotFoundError",
  { artifactId: ArtifactId },
) {}
