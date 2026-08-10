import { Schema } from "effect";
import { WorkflowRunError } from "./workflow-run-error.js";

export const WorkflowRunState = Schema.TaggedUnion({
  Pending: {},
  Success: { value: Schema.Json },
  Failure: { error: WorkflowRunError },
  Interrupted: {},
  Defect: { message: Schema.String },
  Suspended: {},
});
export type WorkflowRunState = typeof WorkflowRunState.Type;
