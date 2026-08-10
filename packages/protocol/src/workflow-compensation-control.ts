import { WorkflowRunAddress } from "@cvr/loom-domain";
import { Schema } from "effect";
import { Rpc } from "effect/unstable/rpc";
import { WorkflowCompensationDecisionConflictError } from "./workflow-compensation-decision-conflict-error.js";
import { WorkflowCompensationDecisionTimeoutError } from "./workflow-compensation-decision-timeout-error.js";
import { WorkflowCompensationDecision } from "./workflow-compensation-decision.js";
import { WorkflowCompensationNotPendingError } from "./workflow-compensation-not-pending-error.js";
import { WorkflowRunAcceptanceError } from "./workflow-run-acceptance-error.js";
import { WorkflowRunNotFoundError } from "./workflow-run-not-found-error.js";

export const DecideWorkflowCompensationRequest = Schema.Struct({
  address: WorkflowRunAddress,
  decision: WorkflowCompensationDecision,
});
export type DecideWorkflowCompensationRequest = typeof DecideWorkflowCompensationRequest.Type;

export const DecideWorkflowCompensationError = Schema.Union([
  WorkflowRunNotFoundError,
  WorkflowRunAcceptanceError,
  WorkflowCompensationNotPendingError,
  WorkflowCompensationDecisionConflictError,
  WorkflowCompensationDecisionTimeoutError,
]);
export type DecideWorkflowCompensationError = typeof DecideWorkflowCompensationError.Type;

export const DecideWorkflowCompensation = Rpc.make("Workflow.DecideCompensation", {
  payload: DecideWorkflowCompensationRequest,
  error: DecideWorkflowCompensationError,
});
