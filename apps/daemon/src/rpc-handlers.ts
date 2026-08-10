import { WorkflowRunHandle, LoomRpcs } from "@cvr/loom-protocol";
import { AgentActor, ConnectionHandshake, WorkflowRuntime } from "@cvr/loom-runtime";
import { Effect } from "effect";

export const layerLoomRpcHandlers = LoomRpcs.toLayer(
  Effect.gen(function* () {
    const connection = yield* ConnectionHandshake;
    const workflows = yield* WorkflowRuntime;
    return LoomRpcs.of({
      "Connection.Handshake": connection.handshake,
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
