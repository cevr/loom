/* oxlint-disable effect/noGlobals, effect/noNodeBuiltinImport -- This adapter owns the unmatched Bun and VM APIs. */
import { CellId } from "@cvr/loom-domain";
import {
  CellCompilationError,
  CellEvaluation,
  type CellEvaluationError,
  CellExecutionError,
} from "@cvr/loom-protocol";
import { Context, Effect, Inspectable, Layer, Predicate, Semaphore } from "effect";
import * as NodeVm from "node:vm";

export interface EvaluateCellInput {
  readonly cellId: CellId;
  readonly source: string;
}

export interface InProcessCodeKernelShape {
  readonly evaluate: (
    input: EvaluateCellInput,
  ) => Effect.Effect<CellEvaluation, CellEvaluationError>;
  readonly reset: Effect.Effect<void>;
}

export class InProcessCodeKernel extends Context.Service<
  InProcessCodeKernel,
  InProcessCodeKernelShape
>()("@cvr/loom-platform-bun/InProcessCodeKernel") {}

const makeContext = (): NodeVm.Context => NodeVm.createContext({ Bun });

const messageFromCause = (cause: unknown): string => Inspectable.toStringUnknown(cause);

const importModule = (specifier: string) => import(specifier);

const valueFromEvaluation = (result: unknown): unknown => {
  if (Predicate.hasProperty(result, "value")) {
    return result.value;
  }
  return undefined;
};

const awaitEvaluation = (
  cellId: CellId,
  result: unknown,
): Effect.Effect<unknown, CellExecutionError> =>
  Effect.gen(function* () {
    if (!Predicate.isPromiseLike(result)) {
      return result;
    }
    return yield* Effect.tryPromise({
      try: () => result,
      catch: (cause) =>
        CellExecutionError.make({
          cellId,
          message: messageFromCause(cause),
        }),
    });
  });

const makeTranspiler = (): Bun.Transpiler =>
  new Bun.Transpiler({
    loader: "ts",
    target: "bun",
    replMode: true,
  });

const evaluateSource = (
  transpiler: Bun.Transpiler,
  context: NodeVm.Context,
  input: EvaluateCellInput,
): Effect.Effect<unknown, CellEvaluationError> =>
  Effect.gen(function* () {
    const source = yield* Effect.try({
      try: () => transpiler.transformSync(input.source),
      catch: (cause) =>
        CellCompilationError.make({
          cellId: input.cellId,
          message: messageFromCause(cause),
        }),
    });
    const raw = yield* Effect.try({
      try: () => NodeVm.runInContext(source, context, { importModuleDynamically: importModule }),
      catch: (cause) =>
        CellExecutionError.make({
          cellId: input.cellId,
          message: messageFromCause(cause),
        }),
    });
    return yield* awaitEvaluation(input.cellId, raw);
  });

export const makeInProcessCodeKernel: Effect.Effect<InProcessCodeKernelShape> = Effect.gen(
  function* () {
    const transpiler = makeTranspiler();
    const semaphore = yield* Semaphore.make(1);
    let context = makeContext();

    const evaluateInContext = (
      input: EvaluateCellInput,
    ): Effect.Effect<CellEvaluation, CellEvaluationError> =>
      Effect.gen(function* () {
        const evaluation = yield* evaluateSource(transpiler, context, input);
        const value = valueFromEvaluation(evaluation);

        return CellEvaluation.make({
          cellId: input.cellId,
          display: Bun.inspect(value),
          bindings: Reflect.ownKeys(context).filter(Predicate.isString).toSorted(),
        });
      });

    return {
      evaluate: (input) => Semaphore.withPermit(semaphore, evaluateInContext(input)),
      reset: Semaphore.withPermit(
        semaphore,
        Effect.sync(() => {
          context = makeContext();
        }),
      ),
    };
  },
);

export const layerInProcessCodeKernel: Layer.Layer<InProcessCodeKernel> = Layer.effect(
  InProcessCodeKernel,
  makeInProcessCodeKernel,
);
