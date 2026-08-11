import { Schema } from "effect";

const identifier = (name: string) => Schema.NonEmptyString.pipe(Schema.brand(name));

export const SessionId = identifier("@cvr/loom/SessionId");
export type SessionId = typeof SessionId.Type;

export const WorkspaceRoot = identifier("@cvr/loom/WorkspaceRoot");
export type WorkspaceRoot = typeof WorkspaceRoot.Type;

export const AgentId = identifier("@cvr/loom/AgentId");
export type AgentId = typeof AgentId.Type;

export const CellId = identifier("@cvr/loom/CellId");
export type CellId = typeof CellId.Type;

export const JobId = identifier("@cvr/loom/JobId");
export type JobId = typeof JobId.Type;

export const WorkflowRunId = identifier("@cvr/loom/WorkflowRunId");
export type WorkflowRunId = typeof WorkflowRunId.Type;

export const WorkflowIncarnationId = identifier("@cvr/loom/WorkflowIncarnationId");
export type WorkflowIncarnationId = typeof WorkflowIncarnationId.Type;

export const WorkflowName = identifier("@cvr/loom/WorkflowName");
export type WorkflowName = typeof WorkflowName.Type;

export const WorkflowVersion = identifier("@cvr/loom/WorkflowVersion");
export type WorkflowVersion = typeof WorkflowVersion.Type;

export const WorkflowKey = identifier("@cvr/loom/WorkflowKey");
export type WorkflowKey = typeof WorkflowKey.Type;

export const WorkflowCapability = identifier("@cvr/loom/WorkflowCapability");
export type WorkflowCapability = typeof WorkflowCapability.Type;

export const WorkflowStepId = identifier("@cvr/loom/WorkflowStepId");
export type WorkflowStepId = typeof WorkflowStepId.Type;

export const WorkflowActivityKey = identifier("@cvr/loom/WorkflowActivityKey");
export type WorkflowActivityKey = typeof WorkflowActivityKey.Type;

export const WorkflowSignalName = identifier("@cvr/loom/WorkflowSignalName");
export type WorkflowSignalName = typeof WorkflowSignalName.Type;

export const ArtifactId = identifier("@cvr/loom/ArtifactId");
export type ArtifactId = typeof ArtifactId.Type;

export const workflowAgentId = (activityKey: WorkflowActivityKey): AgentId =>
  AgentId.make(`workflow-agent:${activityKey}`);

export const workflowAgentJobId = (activityKey: WorkflowActivityKey): JobId =>
  JobId.make(`workflow-agent-job:${activityKey}`);

export const workflowJobId = (activityKey: WorkflowActivityKey): JobId =>
  JobId.make(`workflow-job:${activityKey}`);

export const workflowArtifactId = (activityKey: WorkflowActivityKey): ArtifactId =>
  ArtifactId.make(`workflow-artifact:${activityKey}`);
