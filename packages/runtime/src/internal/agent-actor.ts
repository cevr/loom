import { AgentId, type AgentOwner, SessionId } from "@cvr/loom-domain";
import {
  CellEvaluation,
  CellEvaluationError,
  CellInterruptedError,
  EvaluateCellRequest,
} from "@cvr/loom-protocol";
import { Effect, Schema } from "effect";
import { Actor } from "effect-encore";
import { CellJournal } from "./cell-journal.js";
import { CodeKernelFactory } from "./code-kernel-factory.js";

const AgentOwnerKey = Schema.Tuple([SessionId, AgentId]);
const agentOwnerCodec = Actor.entityIdCodec(AgentOwnerKey);

export const agentEntityId = (owner: AgentOwner): string =>
  agentOwnerCodec.encode([owner.sessionId, owner.agentId]);

export const AgentActor = Actor.fromEntity("LoomAgent", {
  EvaluateCell: {
    payload: EvaluateCellRequest,
    success: CellEvaluation,
    error: CellEvaluationError,
    id: agentEntityId,
  },
  ResetCodeKernel: {
    payload: { sessionId: SessionId, agentId: AgentId },
    id: agentEntityId,
  },
  CloseCodeKernel: {
    payload: { sessionId: SessionId, agentId: AgentId },
    id: agentEntityId,
  },
});

export const layerAgentActor = Actor.toLayer(
  AgentActor,
  Effect.gen(function* () {
    const factory = yield* CodeKernelFactory;
    const kernel = yield* factory.spawn;
    const journal = yield* CellJournal;

    return AgentActor.of({
      EvaluateCell: ({ operation }) =>
        journal
          .append({
            sessionId: operation.sessionId,
            agentId: operation.agentId,
            cellId: operation.cellId,
            source: operation.source,
          })
          .pipe(
            Effect.mapError(
              () =>
                new CellInterruptedError({
                  cellId: operation.cellId,
                  reason: "JournalFailure",
                  message: "Loom could not store the Cell before evaluation.",
                }),
            ),
            Effect.andThen(kernel.evaluate({ cellId: operation.cellId, source: operation.source })),
          ),
      ResetCodeKernel: () => kernel.reset,
      CloseCodeKernel: () => kernel.close,
    });
  }),
  { concurrency: 1 },
);
