import { CellInterruptedError, type EvaluateCellRequest, LoomRpcs } from "@cvr/loom-protocol";
import { CodeKernel } from "@cvr/loom-platform-bun";
import { CellJournal, ConnectionHandshake } from "@cvr/loom-runtime";
import { Effect } from "effect";

export const evaluateJournaledCell = Effect.fn("LoomDaemon.evaluateJournaledCell")(function* (
  request: EvaluateCellRequest,
) {
  const kernel = yield* CodeKernel;
  const journal = yield* CellJournal;
  yield* journal
    .append({
      sessionId: request.sessionId,
      agentId: request.agentId,
      cellId: request.cellId,
      source: request.source,
    })
    .pipe(
      Effect.mapError(
        () =>
          new CellInterruptedError({
            cellId: request.cellId,
            reason: "JournalFailure",
            message: "Loom could not store the Cell before evaluation.",
          }),
      ),
    );
  return yield* kernel.evaluate({ cellId: request.cellId, source: request.source });
});

export const layerLoomRpcHandlers = LoomRpcs.toLayer(
  Effect.gen(function* () {
    const connection = yield* ConnectionHandshake;
    const kernel = yield* CodeKernel;
    const journal = yield* CellJournal;
    return LoomRpcs.of({
      "Connection.Handshake": connection.handshake,
      "CodeKernel.EvaluateCell": (request) =>
        evaluateJournaledCell(request).pipe(
          Effect.provideService(CodeKernel, kernel),
          Effect.provideService(CellJournal, journal),
        ),
      "CodeKernel.Reset": () => kernel.reset,
    });
  }),
);
