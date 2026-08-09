import { WorkspaceRoot } from "@cvr/loom-domain";
import { Schema } from "effect";

export class WorkspaceMismatchError extends Schema.TaggedError<WorkspaceMismatchError>()(
  "WorkspaceMismatchError",
  {
    expected: WorkspaceRoot,
    received: WorkspaceRoot,
  },
) {}
