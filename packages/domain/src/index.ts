import { Schema } from "effect";

const identifier = (name: string) => Schema.NonEmptyString.pipe(Schema.brand(name));

export const SessionId = identifier("@cvr/loom/SessionId");
export type SessionId = typeof SessionId.Type;

export const AgentId = identifier("@cvr/loom/AgentId");
export type AgentId = typeof AgentId.Type;

export const CellId = identifier("@cvr/loom/CellId");
export type CellId = typeof CellId.Type;

export const JobId = identifier("@cvr/loom/JobId");
export type JobId = typeof JobId.Type;

export const WorkflowRunId = identifier("@cvr/loom/WorkflowRunId");
export type WorkflowRunId = typeof WorkflowRunId.Type;

export const ArtifactId = identifier("@cvr/loom/ArtifactId");
export type ArtifactId = typeof ArtifactId.Type;

export const AgentOwner = Schema.Struct({
  sessionId: SessionId,
  agentId: AgentId,
});
export type AgentOwner = typeof AgentOwner.Type;
