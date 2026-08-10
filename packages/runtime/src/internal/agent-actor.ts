import { AgentId, type AgentOwner, SessionId } from "@cvr/loom-domain";
import {
  CellIdentityConflictError,
  CellLedgerEntry,
  CellLedgerState,
  CellEvaluation,
  CellEvaluationError,
  CellInterruptedError,
  EvaluateCellRequest,
} from "@cvr/loom-protocol";
import { Effect, Schema } from "effect";
import { Actor } from "effect-encore";
import { CellLedger, CellLedgerClaim, type CellLedgerShape } from "./cell-ledger.js";
import type { CodeKernelShape } from "./code-kernel.js";
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

const ledgerFailure = (operation: EvaluateCellRequest, message: string) =>
  new CellInterruptedError({ cellId: operation.cellId, reason: "JournalFailure", message });

const readExisting = Effect.fn("AgentActor.readExisting")(function* (
  operation: EvaluateCellRequest,
  entry: CellLedgerEntry,
) {
  if (entry.source !== operation.source) {
    return yield* new CellIdentityConflictError({
      sessionId: operation.sessionId,
      agentId: operation.agentId,
      cellId: operation.cellId,
    });
  }
  return yield* CellLedgerState.match<Effect.Effect<CellEvaluation, CellEvaluationError>>(
    entry.state,
    {
      Accepted: () =>
        Effect.fail(
          new CellInterruptedError({
            cellId: operation.cellId,
            reason: "EvaluationInProgress",
            message: "This Cell is already accepted for evaluation.",
          }),
        ),
      Evaluating: () =>
        Effect.fail(
          new CellInterruptedError({
            cellId: operation.cellId,
            reason: "EvaluationInProgress",
            message: "This Cell is already evaluating.",
          }),
        ),
      Succeeded: ({ evaluation }) => Effect.succeed(evaluation),
      Failed: ({ error }) => Effect.fail(error),
      Interrupted: ({ error }) => Effect.fail(error),
    },
  );
});

const evaluateAccepted = Effect.fn("AgentActor.evaluateAccepted")(function* (
  ledger: CellLedgerShape,
  kernel: CodeKernelShape,
  operation: EvaluateCellRequest,
  entry: CellLedgerEntry,
) {
  yield* ledger.evaluating(entry);
  return yield* kernel.evaluate({ cellId: operation.cellId, source: operation.source }).pipe(
    Effect.matchEffect({
      onFailure: (error) => ledger.complete(entry, error).pipe(Effect.andThen(Effect.fail(error))),
      onSuccess: (evaluation) => ledger.complete(entry, evaluation).pipe(Effect.as(evaluation)),
    }),
  );
});

const makeEvaluateCell = (ledger: CellLedgerShape, kernel: CodeKernelShape) =>
  Effect.fn("AgentActor.evaluateCell")(function* (operation: EvaluateCellRequest) {
    const claim = yield* ledger
      .claim(
        CellLedgerEntry.make({
          sessionId: operation.sessionId,
          agentId: operation.agentId,
          cellId: operation.cellId,
          source: operation.source,
          state: CellLedgerState.cases.Accepted.make({}),
        }),
      )
      .pipe(
        Effect.mapError(() =>
          ledgerFailure(operation, "Loom could not store the Cell before evaluation."),
        ),
      );
    return yield* CellLedgerClaim.$match(claim, {
      Accepted: ({ entry }) => evaluateAccepted(ledger, kernel, operation, entry),
      Existing: ({ entry }) => readExisting(operation, entry),
    }).pipe(
      Effect.catchTag("CellLedgerStoreError", (error) =>
        Effect.logError("Cell Ledger write failed.", error).pipe(
          Effect.andThen(
            Effect.fail(ledgerFailure(operation, "Loom could not store the Cell outcome.")),
          ),
        ),
      ),
    );
  });

export const layerAgentActor = Actor.toLayer(
  AgentActor,
  Effect.gen(function* () {
    const factory = yield* CodeKernelFactory;
    const address = yield* Actor.CurrentAddress;
    const [sessionId, agentId] = yield* agentOwnerCodec.decode(address.entityId).pipe(Effect.orDie);
    const kernel = yield* factory.spawn({ sessionId, agentId });
    const ledger = yield* CellLedger;

    return AgentActor.of({
      EvaluateCell: ({ operation }) => makeEvaluateCell(ledger, kernel)(operation),
      ResetCodeKernel: () => kernel.reset,
      CloseCodeKernel: () => kernel.close,
    });
  }),
  { concurrency: 1 },
);
