import { Schema } from "effect";
import { CellCompilationError } from "./cell-compilation-error.js";
import { CellExecutionError } from "./cell-execution-error.js";
import { CellInterruptedError } from "./cell-interrupted-error.js";
import { CellIdentityConflictError } from "./cell-identity-conflict-error.js";

export const CellKernelError = Schema.Union([
  CellCompilationError,
  CellExecutionError,
  CellInterruptedError,
]);
export type CellKernelError = typeof CellKernelError.Type;

export const CellEvaluationError = Schema.Union([CellKernelError, CellIdentityConflictError]);
export type CellEvaluationError = typeof CellEvaluationError.Type;
