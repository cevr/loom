import { CloseSessionError, LoomRpcs, WorkflowRunHandle } from "@cvr/loom-protocol";
import {
  AgentActor,
  ActorStateHub,
  ConnectionHandshake,
  JobRuntime,
  WorkflowChildAgentStore,
  WorkflowRuntime,
} from "@cvr/loom-runtime";
import { Effect, Inspectable, Stream } from "effect";
import { makeJobRpcHandlers } from "./job-rpc-handlers.js";

export const layerLoomRpcHandlers = LoomRpcs.toLayer(
  Effect.gen(function* () {
    const connection = yield* ConnectionHandshake;
    const actors = yield* ActorStateHub;
    const workflows = yield* WorkflowRuntime;
    const childAgents = yield* WorkflowChildAgentStore;
    const jobs = yield* JobRuntime;
    return LoomRpcs.of({
      ...makeJobRpcHandlers(jobs),
      "Connection.Handshake": connection.handshake,
      "Session.Close": ({ sessionId }) =>
        Effect.all([childAgents.stopSession(sessionId), jobs.closeSession(sessionId)], {
          discard: true,
        }).pipe(
          Effect.mapError(
            (error) =>
              new CloseSessionError({
                sessionId,
                message: Inspectable.toStringUnknown(error),
              }),
          ),
        ),
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
