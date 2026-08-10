import { WorkflowCapability } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkflowCapabilityDeniedError extends Schema.TaggedError<WorkflowCapabilityDeniedError>()(
  "WorkflowCapabilityDeniedError",
  { capability: WorkflowCapability },
) {}
