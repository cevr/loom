import type { WorkflowRunId, WorkflowSignalName } from "@cvr/loom-domain";
import type { WorkflowSignalDeclarationsError } from "@cvr/loom-protocol";
import { Context, type Effect } from "effect";

export interface WorkflowSignalDeclarationsShape {
  readonly declare: (
    workflowRunId: WorkflowRunId,
    names: ReadonlyArray<WorkflowSignalName>,
  ) => Effect.Effect<void, WorkflowSignalDeclarationsError>;
  readonly contains: (
    workflowRunId: WorkflowRunId,
    name: WorkflowSignalName,
  ) => Effect.Effect<boolean, WorkflowSignalDeclarationsError>;
}

export class WorkflowSignalDeclarations extends Context.Service<
  WorkflowSignalDeclarations,
  WorkflowSignalDeclarationsShape
>()("@cvr/loom-runtime/WorkflowSignalDeclarations") {}
