import {
  CellInterruptedError,
  CodeKernelProcessRequest,
  CodeKernelProcessResponse,
} from "@cvr/loom-protocol";
import {
  CodeKernel,
  CodeKernelFactory,
  type CodeKernelShape,
  type EvaluateCellInput,
} from "@cvr/loom-runtime";
import { Duration, Effect, Exit, FileSystem, Layer, Path, Scope, Semaphore } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import {
  sendKernelRequest,
  spawnKernelChild,
  type CodeKernelProcessTransportConfig,
  type KernelChild,
  type ReserveKernelDiagnostic,
} from "./code-kernel-process-transport.js";
import { failWithDiagnostic } from "./code-kernel-diagnostics.js";
import {
  makeCodeKernelDiagnosticStore,
  type CodeKernelDiagnosticStoreConfig,
} from "./code-kernel-diagnostic-store.js";
import type { CodeKernelProcessError } from "./code-kernel-process-error.js";
import {
  assertStartAllowed,
  clearProcessFailures,
  recordProcessFailure,
  type CodeKernelSupervisorPolicyConfig,
  type CodeKernelSupervisorPolicyState,
} from "./code-kernel-supervisor-policy.js";

export interface CodeKernelProcessConfig
  extends CodeKernelProcessTransportConfig, CodeKernelSupervisorPolicyConfig {
  readonly cellTimeout?: Duration.Input;
}

interface KernelSupervisorState extends CodeKernelSupervisorPolicyState {
  child: KernelChild | undefined;
  nextRequestId: number;
  readonly parentScope: Scope.Scope;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly fs: FileSystem.FileSystem;
  readonly reserveDiagnostic: ReserveKernelDiagnostic | undefined;
}

const getChild = Effect.fn("CodeKernelProcess.getChild")(function* (
  config: CodeKernelProcessConfig,
  state: KernelSupervisorState,
) {
  if (state.child !== undefined) return state.child;
  yield* assertStartAllowed(state);
  const child = yield* spawnKernelChild(
    config,
    state.parentScope,
    state.spawner,
    state.fs,
    state.reserveDiagnostic,
  );
  state.child = child;
  return child;
});

const replaceChild = Effect.fn("CodeKernelProcess.replace")(function* (
  state: KernelSupervisorState,
) {
  if (state.child === undefined) return;
  yield* Scope.close(state.child.scope, Exit.void);
  state.child = undefined;
  state.nextRequestId = 0;
});

const replaceFailedChild = Effect.fn("CodeKernelProcess.replaceFailed")(function* (
  config: CodeKernelProcessConfig,
  state: KernelSupervisorState,
  error: CodeKernelProcessError,
) {
  yield* replaceChild(state);
  if (error.reason !== "CrashLoop") yield* recordProcessFailure(config, state);
});

const evaluateCell = Effect.fn("CodeKernel.evaluate")(function* (
  config: CodeKernelProcessConfig,
  state: KernelSupervisorState,
  cellTimeout: Duration.Input,
  input: EvaluateCellInput,
) {
  const active = yield* getChild(config, state);
  state.nextRequestId += 1;
  const requestId = state.nextRequestId;
  const response = yield* sendKernelRequest(
    active,
    CodeKernelProcessRequest.cases.Evaluate.make({
      requestId,
      cellId: input.cellId,
      source: input.source,
    }),
    cellTimeout,
  );
  if (CodeKernelProcessResponse.guards.EvaluationSucceeded(response)) {
    yield* clearProcessFailures(state);
    return response.evaluation;
  }
  if (CodeKernelProcessResponse.guards.EvaluationFailed(response)) {
    yield* clearProcessFailures(state);
    return yield* response.error;
  }
  return yield* failWithDiagnostic(
    active.diagnostics,
    "ProtocolFailure",
    "Code Kernel returned a reset response for a Cell.",
    response,
    requestId,
  );
});

const makeEvaluate =
  (config: CodeKernelProcessConfig, state: KernelSupervisorState, cellTimeout: Duration.Input) =>
  (input: EvaluateCellInput) =>
    evaluateCell(config, state, cellTimeout, input).pipe(
      Effect.onInterrupt(() => Effect.uninterruptible(replaceChild(state))),
      Effect.catchTag("CodeKernelProcessError", (error) =>
        replaceFailedChild(config, state, error).pipe(
          Effect.andThen(
            new CellInterruptedError({
              cellId: input.cellId,
              reason: error.reason,
              message: error.message,
              diagnostic: error.diagnostic,
            }),
          ),
        ),
      ),
    );

const makeReset = (
  config: CodeKernelProcessConfig,
  state: KernelSupervisorState,
  cellTimeout: Duration.Input,
) => {
  const reset = Effect.fn("CodeKernel.reset")(function* () {
    if (state.child === undefined) return;
    const active = state.child;
    state.nextRequestId += 1;
    const requestId = state.nextRequestId;
    const response = yield* sendKernelRequest(
      active,
      CodeKernelProcessRequest.cases.Reset.make({ requestId }),
      cellTimeout,
    );
    if (!CodeKernelProcessResponse.guards.ResetSucceeded(response)) {
      return yield* failWithDiagnostic(
        active.diagnostics,
        "ProtocolFailure",
        "Code Kernel returned an evaluation response for a reset.",
        response,
        requestId,
      );
    }
    yield* clearProcessFailures(state);
  });

  return reset().pipe(
    Effect.onInterrupt(() => Effect.uninterruptible(replaceChild(state))),
    Effect.catchTag("CodeKernelProcessError", (error) => replaceFailedChild(config, state, error)),
  );
};

const makeCodeKernelWithDiagnostic = (
  config: CodeKernelProcessConfig,
  reserveDiagnostic?: ReserveKernelDiagnostic,
): Effect.Effect<
  CodeKernelShape,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Scope.Scope
> =>
  Effect.gen(function* () {
    const parentScope = yield* Scope.Scope;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const fs = yield* FileSystem.FileSystem;
    const semaphore = yield* Semaphore.make(1);
    const cellTimeout = config.cellTimeout ?? "30 seconds";
    const state: KernelSupervisorState = {
      child: undefined,
      nextRequestId: 0,
      failureTimes: [],
      blockedUntil: undefined,
      parentScope,
      spawner,
      fs,
      reserveDiagnostic,
    };
    const evaluate = makeEvaluate(config, state, cellTimeout);
    const reset = makeReset(config, state, cellTimeout);

    return CodeKernel.of({
      evaluate: (input) => Semaphore.withPermit(semaphore, evaluate(input)),
      reset: Semaphore.withPermit(semaphore, reset),
      close: Semaphore.withPermit(
        semaphore,
        replaceChild(state).pipe(Effect.andThen(clearProcessFailures(state))),
      ),
    });
  });

export const makeCodeKernel = (config: CodeKernelProcessConfig) =>
  makeCodeKernelWithDiagnostic(config);

export const layerCodeKernel = (
  config: CodeKernelProcessConfig,
): Layer.Layer<
  CodeKernel,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem
> => Layer.effect(CodeKernel, makeCodeKernel(config));

export interface CodeKernelFactoryConfig
  extends CodeKernelProcessConfig, CodeKernelDiagnosticStoreConfig {}

export const layerCodeKernelFactory = (
  config: CodeKernelFactoryConfig,
): Layer.Layer<
  CodeKernelFactory,
  never,
  ChildProcessSpawner.ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> =>
  Layer.effect(
    CodeKernelFactory,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const fs = yield* FileSystem.FileSystem;
      const store = yield* makeCodeKernelDiagnosticStore(config);
      return CodeKernelFactory.of({
        spawn: (owner) => {
          let reserveDiagnostic: ReserveKernelDiagnostic | undefined;
          if (store !== undefined) reserveDiagnostic = (pid) => store.reserve(owner, pid);
          return makeCodeKernelWithDiagnostic(config, reserveDiagnostic).pipe(
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
            Effect.provideService(FileSystem.FileSystem, fs),
          );
        },
      });
    }),
  );
