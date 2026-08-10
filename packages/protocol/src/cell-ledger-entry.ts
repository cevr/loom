import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { Schema } from "effect";
import { CellCompilationError } from "./cell-compilation-error.js";
import { CellEvaluation } from "./cell-evaluation.js";
import { CellExecutionError } from "./cell-execution-error.js";
import { CellInterruptedError } from "./cell-interrupted-error.js";

export const CellLedgerState = Schema.TaggedUnion({
  Accepted: {},
  Evaluating: {},
  Succeeded: { evaluation: CellEvaluation },
  Failed: { error: Schema.Union([CellCompilationError, CellExecutionError]) },
  Interrupted: { error: CellInterruptedError },
});
export type CellLedgerState = typeof CellLedgerState.Type;

export const CellLedgerEntry = Schema.Struct({
  sessionId: SessionId,
  agentId: AgentId,
  cellId: CellId,
  source: Schema.String,
  state: CellLedgerState,
});
export type CellLedgerEntry = typeof CellLedgerEntry.Type;
