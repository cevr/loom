import { WorkflowIdentity, WorkflowRequestDigest } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowIdentityConflictError extends Schema.TaggedError<WorkflowIdentityConflictError>()(
  "WorkflowIdentityConflictError",
  {
    identity: WorkflowIdentity,
    acceptedDigest: WorkflowRequestDigest,
    receivedDigest: WorkflowRequestDigest,
  },
) {}
