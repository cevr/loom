import { WorkflowRunAddress } from "@cvr/loom-domain";
import { Schema } from "effect";
import { WorkflowRunState } from "./workflow-run-state.js";

export class WorkflowRunNotSuspendedError extends Schema.TaggedError<WorkflowRunNotSuspendedError>()(
  "WorkflowRunNotSuspendedError",
  { address: WorkflowRunAddress, state: WorkflowRunState },
) {}
