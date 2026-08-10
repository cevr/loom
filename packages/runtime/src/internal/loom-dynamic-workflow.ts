import { type WorkflowSignalName, WorkflowRunExecution } from "@cvr/loom-domain";
import { WorkflowRunError } from "@cvr/loom-protocol";
import { Schema } from "effect";
import { Actor } from "effect-encore";

export const LoomDynamicWorkflow = Actor.fromWorkflow("LoomDynamicWorkflow", {
  payload: WorkflowRunExecution.fields,
  success: Schema.Json,
  error: WorkflowRunError,
  id: (execution) => execution.incarnationId,
});

export const loomWorkflowSignal = (name: WorkflowSignalName) =>
  LoomDynamicWorkflow.signal(name, {
    success: Schema.Json,
  });
