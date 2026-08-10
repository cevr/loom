import {
  type WorkflowIdentity,
  WorkflowKey,
  WorkflowName,
  type WorkflowRunRequest,
  WorkflowVersion,
  SessionId,
} from "@cvr/loom-domain";
import { Schema } from "effect";

const WorkflowIdentityTuple = Schema.Tuple([SessionId, WorkflowName, WorkflowVersion, WorkflowKey]);
const encodeIdentityTuple = Schema.encodeSync(Schema.fromJsonString(WorkflowIdentityTuple));

export const workflowIdentityFromRequest = (request: WorkflowRunRequest): WorkflowIdentity => ({
  sessionId: request.sessionId,
  name: request.definition.name,
  version: request.definition.version,
  key: request.key,
});

export const encodeWorkflowIdentity = (identity: WorkflowIdentity): string =>
  encodeIdentityTuple([identity.sessionId, identity.name, identity.version, identity.key]);
