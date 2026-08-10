import {
  ArtifactId,
  WorkflowCapability,
  WorkflowSignalName,
  WorkflowStepId,
} from "@cvr/loom-domain";
import { Schema } from "effect";

export const WorkflowStepCall = Schema.Struct({
  stepId: WorkflowStepId,
  capability: WorkflowCapability,
  input: Schema.Json,
});
export type WorkflowStepCall = typeof WorkflowStepCall.Type;

export const WorkflowHostCall = Schema.TaggedUnion({
  Step: { call: WorkflowStepCall },
  Signal: { name: WorkflowSignalName },
});
export type WorkflowHostCall = typeof WorkflowHostCall.Type;

export const WorkflowStepExecution = Schema.Struct({
  value: Schema.Json,
  tokenCount: Schema.Natural,
  agentRuns: Schema.Natural,
});
export type WorkflowStepExecution = typeof WorkflowStepExecution.Type;

export const WorkflowArtifactReference = Schema.TaggedStruct("Artifact", {
  artifactId: ArtifactId,
});
export type WorkflowArtifactReference = typeof WorkflowArtifactReference.Type;

export const WorkflowArtifactWrite = Schema.Struct({
  stepId: WorkflowStepId,
  value: Schema.Json,
});
export type WorkflowArtifactWrite = typeof WorkflowArtifactWrite.Type;
