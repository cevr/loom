import { Rpc } from "effect/unstable/rpc";
import { CellEvaluation } from "./cell-evaluation.js";
import { CellEvaluationError } from "./cell-evaluation-error.js";
import { EvaluateCellRequest } from "./evaluate-cell-request.js";

export class EvaluateCell extends Rpc.make("CodeKernel.EvaluateCell", {
  payload: EvaluateCellRequest,
  success: CellEvaluation,
  error: CellEvaluationError,
}) {}
