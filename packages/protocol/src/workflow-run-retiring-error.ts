import { WorkflowIdentity, WorkflowRunId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowRunRetiringError extends Schema.TaggedError<WorkflowRunRetiringError>()(
  "WorkflowRunRetiringError",
  {
    identity: WorkflowIdentity,
    workflowRunId: WorkflowRunId,
  },
) {}
