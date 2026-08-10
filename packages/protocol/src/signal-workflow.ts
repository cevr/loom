import { WorkflowSignalAddress } from "@cvr/loom-domain";
import { Schema } from "effect";
import { Rpc } from "effect/unstable/rpc";
import { WorkflowSignalDeclarationsError } from "./workflow-signal-declarations-error.js";
import { WorkflowSignalNotDeclaredError } from "./workflow-signal-not-declared-error.js";

export const SignalWorkflowRequest = Schema.Struct({
  address: WorkflowSignalAddress,
  value: Schema.Json,
});
export type SignalWorkflowRequest = typeof SignalWorkflowRequest.Type;

export const SignalWorkflowError = Schema.Union([
  WorkflowSignalNotDeclaredError,
  WorkflowSignalDeclarationsError,
]);
export type SignalWorkflowError = typeof SignalWorkflowError.Type;

export class SignalWorkflow extends Rpc.make("Workflow.Signal", {
  payload: SignalWorkflowRequest,
  error: SignalWorkflowError,
}) {}
