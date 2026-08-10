import { CloseSessionError, LoomRpcs, WorkflowRunHandle } from "@cvr/loom-protocol";
import {
  AgentActor,
  ConnectionHandshake,
  WorkflowChildAgentStore,
  WorkflowRuntime,
} from "@cvr/loom-runtime";
import { Effect } from "effect";

export const layerLoomRpcHandlers = LoomRpcs.toLayer(
  Effect.gen(function* () {
    const connection = yield* ConnectionHandshake;
    const workflows = yield* WorkflowRuntime;
    const childAgents = yield* WorkflowChildAgentStore;
    return LoomRpcs.of({
      "Connection.Handshake": connection.handshake,
      "Session.Close": ({ sessionId }) =>
        childAgents
          .stopSession(sessionId)
          .pipe(
            Effect.mapError(
              (error) => new CloseSessionError({ sessionId, message: error.message }),
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
    });
  }),
);
