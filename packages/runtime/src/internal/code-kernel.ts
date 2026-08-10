import type { CellEvaluation, CellKernelError } from "@cvr/loom-protocol";
import { Context, type Effect } from "effect";

export interface EvaluateCellInput {
  readonly cellId: CellEvaluation["cellId"];
  readonly source: string;
}

export interface CodeKernelShape {
  readonly evaluate: (input: EvaluateCellInput) => Effect.Effect<CellEvaluation, CellKernelError>;
  readonly reset: Effect.Effect<void>;
  readonly close: Effect.Effect<void>;
}

export class CodeKernel extends Context.Service<CodeKernel, CodeKernelShape>()(
  "@cvr/loom-runtime/CodeKernel",
) {}
