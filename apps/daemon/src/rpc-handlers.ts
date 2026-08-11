import { CloseSessionError, LoomRpcs, WorkflowRunHandle } from "@cvr/loom-protocol";
import type { SessionId } from "@cvr/loom-domain";
import {
  AgentActor,
  ActorStateHub,
  ConnectionHandshake,
  JobRuntime,
  SessionLifecycle,
  WorkflowChildAgentStore,
  WorkflowRuntime,
  type JobRuntimeShape,
  type SessionLifecycleShape,
  type WorkflowChildAgentStoreShape,
  type WorkflowRuntimeShape,
} from "@cvr/loom-runtime";
import { Effect, Inspectable, Stream } from "effect";
import { makeJobRpcHandlers } from "./job-rpc-handlers.js";

const makeCloseSession =
  (
    workflows: WorkflowRuntimeShape,
    childAgents: WorkflowChildAgentStoreShape,
    jobs: JobRuntimeShape,
    sessions: SessionLifecycleShape,
  ) =>
  (sessionId: SessionId) =>
    sessions
      .close(
        sessionId,
        workflows.closeSession(sessionId).pipe(
          // Collect each result so one cleanup failure does not skip the remaining cleanup.
          Effect.result,
          Effect.flatMap((workflowResult) =>
            Effect.all([childAgents.stopSession(sessionId), jobs.closeSession(sessionId)], {
              concurrency: "unbounded",
              mode: "result",
            }).pipe(
              Effect.flatMap(([childAgentResult, jobResult]) =>
                Effect.all(
                  [
                    Effect.fromResult(workflowResult),
                    Effect.fromResult(childAgentResult),
                    Effect.fromResult(jobResult),
                  ],
                  { discard: true },
                ),
              ),
            ),
          ),
        ),
      )
      .pipe(
        Effect.mapError(
          (error) =>
            new CloseSessionError({
              sessionId,
              message: Inspectable.toStringUnknown(error),
            }),
        ),
      );

export const layerLoomRpcHandlers = LoomRpcs.toLayer(
  Effect.gen(function* () {
    const connection = yield* ConnectionHandshake;
    const actors = yield* ActorStateHub;
    const workflows = yield* WorkflowRuntime;
    const childAgents = yield* WorkflowChildAgentStore;
    const jobs = yield* JobRuntime;
    const sessions = yield* SessionLifecycle;
    const closeSession = makeCloseSession(workflows, childAgents, jobs, sessions);
    return LoomRpcs.of({
      ...makeJobRpcHandlers(jobs, sessions),
      "Connection.Handshake": connection.handshake,
      "Session.Close": ({ sessionId }) => closeSession(sessionId),
      "ActorState.Watch": ({ sessionId }) =>
        actors.snapshots.pipe(
          Stream.map((snapshot) =>
            Array.from(snapshot.values()).filter(
              (projection) => projection.subject.sessionId === sessionId,
            ),
          ),
        ),
      "CodeKernel.EvaluateCell": AgentActor.EvaluateCell.execute,
      "CodeKernel.Reset": AgentActor.ResetCodeKernel.execute,
      "Workflow.Start": (request) =>
        workflows
          .send(request)
          .pipe(Effect.map((workflowRunId) => WorkflowRunHandle.make({ workflowRunId }))),
      "Workflow.Execute": workflows.execute,
      "Workflow.Signal": workflows.signal,
      "Workflow.Inspect": workflows.inspect,
      "Workflow.Interrupt": workflows.interrupt,
      "Workflow.DecideCompensation": workflows.decideCompensation,
    });
  }),
);
