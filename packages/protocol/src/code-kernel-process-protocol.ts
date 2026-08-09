import { CellId } from "@cvr/loom-domain";
import { Schema } from "effect";
import { CellEvaluation } from "./cell-evaluation.js";
import { CellEvaluationError } from "./cell-evaluation-error.js";

export const CodeKernelProcessRequest = Schema.TaggedUnion({
  Evaluate: {
    requestId: Schema.Natural,
    cellId: CellId,
    source: Schema.String,
  },
  Reset: {
    requestId: Schema.Natural,
  },
});
export type CodeKernelProcessRequest = typeof CodeKernelProcessRequest.Type;

export const CodeKernelProcessResponse = Schema.TaggedUnion({
  EvaluationSucceeded: {
    requestId: Schema.Natural,
    evaluation: CellEvaluation,
  },
  EvaluationFailed: {
    requestId: Schema.Natural,
    error: CellEvaluationError,
  },
  ResetSucceeded: {
    requestId: Schema.Natural,
  },
});
export type CodeKernelProcessResponse = typeof CodeKernelProcessResponse.Type;
