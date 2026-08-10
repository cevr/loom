import { Rpc } from "effect/unstable/rpc";
import { WorkflowRunAddress } from "@cvr/loom-domain";
import { Schema } from "effect";
import { WorkflowRunState } from "./workflow-run-state.js";
import { WorkflowRunAcceptanceError } from "./workflow-run-acceptance-error.js";
import { WorkflowRunNotFoundError } from "./workflow-run-not-found-error.js";
import { WorkflowRunNotSuspendedError } from "./workflow-run-not-suspended-error.js";

export const InspectWorkflowError = Schema.Union([
  WorkflowRunNotFoundError,
  WorkflowRunAcceptanceError,
]);
export type InspectWorkflowError = typeof InspectWorkflowError.Type;

export const ResumeWorkflowError = Schema.Union([
  WorkflowRunNotFoundError,
  WorkflowRunNotSuspendedError,
  WorkflowRunAcceptanceError,
]);
export type ResumeWorkflowError = typeof ResumeWorkflowError.Type;

export const InspectWorkflow = Rpc.make("Workflow.Inspect", {
  payload: WorkflowRunAddress,
  success: WorkflowRunState,
  error: InspectWorkflowError,
});

export const InterruptWorkflow = Rpc.make("Workflow.Interrupt", {
  payload: WorkflowRunAddress,
  error: InspectWorkflowError,
});

export const ResumeWorkflow = Rpc.make("Workflow.Resume", {
  payload: WorkflowRunAddress,
  error: ResumeWorkflowError,
});
