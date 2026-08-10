import { WorkflowRunRequest } from "@cvr/loom-domain";
import { WorkflowRunError } from "@cvr/loom-protocol";
import { Schema } from "effect";
import { Actor } from "effect-encore";
import { encodeWorkflowIdentity, workflowIdentityFromRequest } from "./workflow-identity.js";

export const LoomDynamicWorkflow = Actor.fromWorkflow("LoomDynamicWorkflow", {
  payload: WorkflowRunRequest.fields,
  success: Schema.Json,
  error: WorkflowRunError,
  id: (request) => encodeWorkflowIdentity(workflowIdentityFromRequest(request)),
});
