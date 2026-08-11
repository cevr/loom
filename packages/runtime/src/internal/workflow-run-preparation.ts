import {
  type AcceptedWorkflowRun,
  WorkflowRunExecution,
  type WorkflowRunRequest,
} from "@cvr/loom-domain";
import { type StartWorkflowError, WorkflowRunAcceptanceError } from "@cvr/loom-protocol";
import { Effect, Inspectable } from "effect";
import { Client } from "effect-encore";
import { LoomDynamicWorkflow } from "./loom-dynamic-workflow.js";
import { SessionLifecycle } from "./session-lifecycle.js";
import type { WorkflowSignalDeclarations } from "./workflow-signal-declarations.js";
import type { WorkflowRunStatePublisher } from "./workflow-run-state-publisher.js";

type AcceptWorkflow = (
  request: WorkflowRunRequest,
) => Effect.Effect<AcceptedWorkflowRun, StartWorkflowError>;

export const toExecution = (accepted: AcceptedWorkflowRun) =>
  WorkflowRunExecution.make({
    incarnationId: accepted.incarnationId,
    request: accepted.request,
  });

export const makePrepareWorkflow = (
  accept: AcceptWorkflow,
  publisher: WorkflowRunStatePublisher["Service"],
  declarations: WorkflowSignalDeclarations["Service"],
) =>
  Effect.gen(function* () {
    const storageClient = yield* Client;
    const sessions = yield* SessionLifecycle;
    return (request: WorkflowRunRequest) =>
      sessions
        .admit(
          request.sessionId,
          // The claim stays inside admission so close observes every accepted Workflow Run.
          accept(request).pipe(
            Effect.tap((accepted) =>
              declarations.declare(accepted.workflowRunId, accepted.request.definition.signals),
            ),
            Effect.tap((accepted) => LoomDynamicWorkflow.send(toExecution(accepted))),
            storageClient.withTransaction,
            Effect.tap((accepted) =>
              publisher.watch({
                sessionId: accepted.request.sessionId,
                workflowRunId: accepted.workflowRunId,
              }),
            ),
          ),
        )
        .pipe(
          Effect.catchTag(
            "SessionClosureStoreError",
            (error) =>
              new WorkflowRunAcceptanceError({
                operation: "lookup",
                message: Inspectable.toStringUnknown(error.cause),
              }),
          ),
        );
  });

export type PrepareWorkflow = Effect.Success<ReturnType<typeof makePrepareWorkflow>>;
