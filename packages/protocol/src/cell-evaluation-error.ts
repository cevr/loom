import { Schema } from "effect";
import { CellCompilationError } from "./cell-compilation-error.js";
import { CellExecutionError } from "./cell-execution-error.js";
import { CellInterruptedError } from "./cell-interrupted-error.js";

export const CellEvaluationError = Schema.Union([
  CellCompilationError,
  CellExecutionError,
  CellInterruptedError,
]);
export type CellEvaluationError = typeof CellEvaluationError.Type;
