import { WorkflowRunRequest } from "@cvr/loom-domain";
import { Schema } from "effect";
import { Rpc } from "effect/unstable/rpc";
import { WorkflowIdentityConflictError } from "./workflow-identity-conflict-error.js";
import { WorkflowRunAcceptanceError } from "./workflow-run-acceptance-error.js";
import { WorkflowRunError } from "./workflow-run-error.js";
import { WorkflowRunRetiringError } from "./workflow-run-retiring-error.js";
import { WorkflowSignalDeclarationsError } from "./workflow-signal-declarations-error.js";

export const ExecuteWorkflowError = Schema.Union([
  WorkflowIdentityConflictError,
  WorkflowRunAcceptanceError,
  WorkflowRunRetiringError,
  WorkflowSignalDeclarationsError,
  WorkflowRunError,
]);
export type ExecuteWorkflowError = typeof ExecuteWorkflowError.Type;

export class ExecuteWorkflow extends Rpc.make("Workflow.Execute", {
  payload: WorkflowRunRequest,
  success: Schema.Json,
  error: ExecuteWorkflowError,
}) {}
