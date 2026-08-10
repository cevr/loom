import { type WorkflowRunAddress, WorkflowStepId } from "@cvr/loom-domain";
import {
  type DecideWorkflowCompensationRequest,
  WorkflowCompensationDecisionConflictError,
  WorkflowCompensationDecisionTimeoutError,
  WorkflowCompensationNotPendingError,
} from "@cvr/loom-protocol";
import { Effect, Match } from "effect";
import { WorkflowEngine } from "effect/unstable/workflow";
import type { CompensationDecisionError } from "effect-encore";
import { LoomDynamicWorkflow } from "./loom-dynamic-workflow.js";
import type { WorkflowRunAcceptance } from "./workflow-run-acceptance.js";
import type { WorkflowRunStatePublisher } from "./workflow-run-state-publisher.js";

const mapCompensationError = (address: WorkflowRunAddress) =>
  Match.type<CompensationDecisionError>().pipe(
    Match.tagsExhaustive({
      CompensationNotPendingError: () => new WorkflowCompensationNotPendingError({ address }),
      CompensationDecisionConflictError: ({ stepId, attempt, acceptedDecision }) =>
        new WorkflowCompensationDecisionConflictError({
          address,
          stepId: WorkflowStepId.make(stepId),
          attempt,
          acceptedDecision,
        }),
    }),
  );

export const makeDecideCompensation = (
  acceptance: WorkflowRunAcceptance["Service"],
  engine: WorkflowEngine.WorkflowEngine["Service"],
  publisher: WorkflowRunStatePublisher["Service"],
) =>
  Effect.fn("WorkflowRuntime.decideCompensation")(
    function* ({ address, decision }: DecideWorkflowCompensationRequest) {
      yield* acceptance.authorize(address);
      yield* LoomDynamicWorkflow.compensation.decidePending(address.workflowRunId, decision).pipe(
        Effect.mapError(mapCompensationError(address)),
        Effect.timeoutOrElse({
          duration: "5 seconds",
          orElse: () => Effect.fail(new WorkflowCompensationDecisionTimeoutError({ address })),
        }),
      );
      yield* publisher.watch(address);
    },
    Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
  );
