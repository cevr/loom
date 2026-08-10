import type {
  CellCompilationError,
  CellEvaluation,
  CellExecutionError,
  CellInterruptedError,
  CellLedgerEntry,
} from "@cvr/loom-protocol";
import { Context, Data, type Effect } from "effect";
import type { CellLedgerStoreError } from "./cell-ledger-store-error.js";

export type CellLedgerClaim = Data.TaggedEnum<{
  Accepted: { readonly entry: CellLedgerEntry };
  Existing: { readonly entry: CellLedgerEntry };
}>;
export const CellLedgerClaim = Data.taggedEnum<CellLedgerClaim>();

export type CellTerminalOutcome =
  | CellEvaluation
  | CellCompilationError
  | CellExecutionError
  | CellInterruptedError;

export interface CellLedgerShape {
  readonly claim: (entry: CellLedgerEntry) => Effect.Effect<CellLedgerClaim, CellLedgerStoreError>;
  readonly evaluating: (entry: CellLedgerEntry) => Effect.Effect<void, CellLedgerStoreError>;
  readonly complete: (
    entry: CellLedgerEntry,
    outcome: CellTerminalOutcome,
  ) => Effect.Effect<void, CellLedgerStoreError>;
  readonly reconcile: Effect.Effect<void, CellLedgerStoreError>;
}

export class CellLedger extends Context.Service<CellLedger, CellLedgerShape>()(
  "@cvr/loom-runtime/CellLedger",
) {}
