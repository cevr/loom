import { SessionId, WorkflowActivityKey, WorkflowRunId } from "@cvr/loom-domain";
import { Schema } from "effect";

export {
  supportsBuiltInWorkflowCapability,
  workflowAgentCapability,
  workflowArtifactCapability,
  workflowJobCapability,
  WorkflowAgentResult,
  WorkflowAgentInput,
  WorkflowJobHandle,
  WorkflowJobInput,
} from "@cvr/loom-protocol";

export const WorkflowActivityContext = Schema.Struct({
  activityKey: WorkflowActivityKey,
  sessionId: SessionId,
  workflowRunId: WorkflowRunId,
});
export type WorkflowActivityContext = typeof WorkflowActivityContext.Type;
