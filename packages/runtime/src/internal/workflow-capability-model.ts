import { AgentId, JobId, SessionId, WorkflowActivityKey, WorkflowRunId } from "@cvr/loom-domain";
import { Schema } from "effect";

export const WorkflowActivityContext = Schema.Struct({
  activityKey: WorkflowActivityKey,
  sessionId: SessionId,
  workflowRunId: WorkflowRunId,
});
export type WorkflowActivityContext = typeof WorkflowActivityContext.Type;

export const WorkflowAgentInput = Schema.Struct({
  prompt: Schema.NonEmptyString,
});
export type WorkflowAgentInput = typeof WorkflowAgentInput.Type;

export const WorkflowAgentHandle = Schema.TaggedStruct("Agent", {
  agentId: AgentId,
});
export type WorkflowAgentHandle = typeof WorkflowAgentHandle.Type;

export const WorkflowJobInput = Schema.Struct({
  command: Schema.NonEmptyString,
});
export type WorkflowJobInput = typeof WorkflowJobInput.Type;

export const WorkflowJobHandle = Schema.TaggedStruct("Job", {
  jobId: JobId,
});
export type WorkflowJobHandle = typeof WorkflowJobHandle.Type;
