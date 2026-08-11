import { AgentId, JobId, JobOutcome, WorkflowCapability } from "@cvr/loom-domain";
import { Schema } from "effect";

export const workflowAgentCapability = WorkflowCapability.make("agent");
export const workflowArtifactCapability = WorkflowCapability.make("artifact");
export const workflowJobCapability = WorkflowCapability.make("job");

const executableCapabilities = new Set([workflowAgentCapability, workflowJobCapability]);

export const supportsBuiltInWorkflowCapability = (capability: WorkflowCapability) =>
  executableCapabilities.has(capability);

export const WorkflowAgentInput = Schema.Struct({
  prompt: Schema.NonEmptyString,
});
export type WorkflowAgentInput = typeof WorkflowAgentInput.Type;

export const WorkflowAgentResult = Schema.TaggedStruct("Agent", {
  agentId: AgentId,
  outcome: JobOutcome,
  stdout: Schema.String,
  stderr: Schema.String,
});
export type WorkflowAgentResult = typeof WorkflowAgentResult.Type;

export const WorkflowJobInput = Schema.Struct({
  command: Schema.NonEmptyString,
});
export type WorkflowJobInput = typeof WorkflowJobInput.Type;

export const WorkflowJobHandle = Schema.TaggedStruct("Job", {
  jobId: JobId,
});
export type WorkflowJobHandle = typeof WorkflowJobHandle.Type;
