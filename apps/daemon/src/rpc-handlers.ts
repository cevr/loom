import { LoomRpcs } from "@cvr/loom-protocol";
import { AgentActor, ConnectionHandshake } from "@cvr/loom-runtime";
import { Effect } from "effect";

export const layerLoomRpcHandlers = LoomRpcs.toLayer(
  Effect.gen(function* () {
    const connection = yield* ConnectionHandshake;
    return LoomRpcs.of({
      "Connection.Handshake": connection.handshake,
      "CodeKernel.EvaluateCell": AgentActor.EvaluateCell.execute,
      "CodeKernel.Reset": AgentActor.ResetCodeKernel.execute,
    });
  }),
);
