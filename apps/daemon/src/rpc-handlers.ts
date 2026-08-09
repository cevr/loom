import { LoomRpcs } from "@cvr/loom-protocol";
import { CodeKernel } from "@cvr/loom-platform-bun";
import { ConnectionHandshake } from "@cvr/loom-runtime";
import { Effect } from "effect";

export const layerLoomRpcHandlers = LoomRpcs.toLayer(
  Effect.gen(function* () {
    const connection = yield* ConnectionHandshake;
    const kernel = yield* CodeKernel;
    return LoomRpcs.of({
      "Connection.Handshake": connection.handshake,
      "CodeKernel.EvaluateCell": (request) =>
        kernel.evaluate({ cellId: request.cellId, source: request.source }),
      "CodeKernel.Reset": () => kernel.reset,
    });
  }),
);
