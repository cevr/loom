/* oxlint-disable effect/noGlobals, effect/noNodeBuiltinImport -- This adapter owns the unmatched Bun and VM APIs. */
import { CellId, SessionId, type SessionId as SessionIdType } from "@cvr/loom-domain";
import {
  CellCompilationError,
  CellEvaluation,
  type CellKernelError,
  CellExecutionError,
  maximumCellBindings,
  maximumCellDisplayLength,
} from "@cvr/loom-protocol";
import { Context, Effect, Inspectable, Layer, Option, Predicate, Semaphore } from "effect";
import * as NodeUtil from "node:util";
import * as NodeVm from "node:vm";
import { makeCodeKernelControl } from "./code-kernel-control.js";

export interface EvaluateCellInput {
  readonly cellId: CellId;
  readonly source: string;
}

export interface InProcessCodeKernelShape {
  readonly evaluate: (input: EvaluateCellInput) => Effect.Effect<CellEvaluation, CellKernelError>;
  readonly reset: Effect.Effect<void>;
}

export class InProcessCodeKernel extends Context.Service<
  InProcessCodeKernel,
  InProcessCodeKernelShape
>()("@cvr/loom-platform-bun/InProcessCodeKernel") {}

const makeCellConsole = () => {
  let output = "";
  const inspect = (value: unknown) => {
    if (Predicate.isString(value)) return value;
    return NodeUtil.inspect(value);
  };
  const write = (...values: ReadonlyArray<unknown>) => {
    if (output.length >= maximumCellDisplayLength) return;
    const line = values.map(inspect).join(" ");
    if (output.length > 0) output += "\n";
    output += line;
  };
  return {
    value: {
      debug: write,
      error: write,
      info: write,
      log: write,
      warn: write,
    },
    beginCell: () => {
      output = "";
    },
    output: () => output,
  };
};

const makeContext = (loom: object, cellConsole: ReturnType<typeof makeCellConsole>) =>
  NodeVm.createContext({ Bun, console: cellConsole.value, loom });

const messageFromCause = (cause: unknown): string => Inspectable.toStringUnknown(cause);

const importModule = (specifier: string) => import(specifier);

const valueFromEvaluation = (result: unknown): Option.Option<unknown> => {
  if (Predicate.hasProperty(result, "value")) {
    return Option.some(result.value);
  }
  return Option.none();
};

const truncateDisplay = (display: string): string => {
  if (display.length <= maximumCellDisplayLength) return display;
  return `${display.slice(0, maximumCellDisplayLength)}\n[output truncated]`;
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
): Effect.Effect<unknown, CellKernelError> =>
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

export const makeInProcessCodeKernelFor = (
  sessionId: SessionIdType,
): Effect.Effect<InProcessCodeKernelShape> =>
  Effect.gen(function* () {
    const transpiler = makeTranspiler();
    const semaphore = yield* Semaphore.make(1);
    const control = makeCodeKernelControl(sessionId);
    const cellConsole = makeCellConsole();
    let context = makeContext(control.value, cellConsole);

    const evaluateInContext = (
      input: EvaluateCellInput,
    ): Effect.Effect<CellEvaluation, CellKernelError> =>
      Effect.gen(function* () {
        control.beginCell(input.cellId);
        cellConsole.beginCell();
        const startedAt = Date.now();
        const evaluation = yield* evaluateSource(transpiler, context, input);
        const value = valueFromEvaluation(evaluation);

        const displayedValue = Option.match(value, {
          onNone: () => "",
          onSome: (present) => Bun.inspect(present),
        });
        const output = [cellConsole.output(), displayedValue].filter((text) => text.length > 0);

        let display = "undefined";
        if (output.length > 0) display = output.join("\n");
        return CellEvaluation.make({
          cellId: input.cellId,
          display: truncateDisplay(display),
          bindings: Reflect.ownKeys(context)
            .filter(Predicate.isString)
            .toSorted()
            .slice(0, maximumCellBindings),
          durationMillis: Date.now() - startedAt,
          fileChanges: control.fileChanges(),
        });
      });

    return {
      evaluate: (input) => Semaphore.withPermit(semaphore, evaluateInContext(input)),
      reset: Semaphore.withPermit(
        semaphore,
        Effect.sync(() => {
          context = makeContext(control.value, cellConsole);
        }),
      ),
    };
  });

export const makeInProcessCodeKernel = makeInProcessCodeKernelFor(SessionId.make("in-process"));

export const layerInProcessCodeKernel: Layer.Layer<InProcessCodeKernel> = Layer.effect(
  InProcessCodeKernel,
  makeInProcessCodeKernel,
);
