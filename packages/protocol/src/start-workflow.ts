import { WorkflowRunId, WorkflowRunRequest } from "@cvr/loom-domain";
import { Schema } from "effect";
import { Rpc } from "effect/unstable/rpc";
import { WorkflowIdentityConflictError } from "./workflow-identity-conflict-error.js";
import { WorkflowRunAcceptanceError } from "./workflow-run-acceptance-error.js";
import { WorkflowRunRetiringError } from "./workflow-run-retiring-error.js";
import { WorkflowSignalDeclarationsError } from "./workflow-signal-declarations-error.js";
import { SessionClosingError } from "./session-closing-error.js";

export const WorkflowRunHandle = Schema.Struct({ workflowRunId: WorkflowRunId });
export type WorkflowRunHandle = typeof WorkflowRunHandle.Type;

export const StartWorkflowError = Schema.Union([
  WorkflowIdentityConflictError,
  WorkflowRunAcceptanceError,
  WorkflowRunRetiringError,
  WorkflowSignalDeclarationsError,
  SessionClosingError,
]);
export type StartWorkflowError = typeof StartWorkflowError.Type;

export class StartWorkflow extends Rpc.make("Workflow.Start", {
  payload: WorkflowRunRequest,
  success: WorkflowRunHandle,
  error: StartWorkflowError,
}) {}
