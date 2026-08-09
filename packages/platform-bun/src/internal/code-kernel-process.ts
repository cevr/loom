import {
  CellInterruptedError,
  CodeKernelProcessRequest,
  CodeKernelProcessResponse,
  type CellEvaluation,
  type CellEvaluationError,
} from "@cvr/loom-protocol";
import {
  Context,
  Duration,
  Effect,
  Exit,
  Layer,
  Queue,
  Schema,
  Scope,
  Semaphore,
  Stream,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import type { ChildProcessHandle } from "effect/unstable/process/ChildProcessSpawner";
import { parseBunJsonLine } from "./bun-jsonl.js";
import { CodeKernelProcessError } from "./code-kernel-process-error.js";
import type { EvaluateCellInput } from "./code-kernel.js";

export interface CodeKernelShape {
  readonly evaluate: (
    input: EvaluateCellInput,
  ) => Effect.Effect<CellEvaluation, CellEvaluationError>;
  readonly reset: Effect.Effect<void>;
}

export class CodeKernel extends Context.Service<CodeKernel, CodeKernelShape>()(
  "@cvr/loom-platform-bun/CodeKernel",
) {}

export interface CodeKernelProcessConfig {
  readonly entryPath: string;
  readonly cellTimeout?: Duration.Input;
  readonly startupTimeout?: Duration.Input;
}

interface KernelChild {
  readonly scope: Scope.Closeable;
  readonly handle: ChildProcessHandle;
  readonly responses: Queue.Queue<Exit.Exit<CodeKernelProcessResponse, CodeKernelProcessError>>;
}

const decodeResponse = Schema.decodeUnknownEffect(CodeKernelProcessResponse);
const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(CodeKernelProcessRequest));

const processError = (reason: CodeKernelProcessError["reason"], message: string, cause: unknown) =>
  new CodeKernelProcessError({ reason, message, cause });

const listenForResponses = (child: KernelChild) =>
  child.handle.stdout.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.filter((line) => line.length > 0),
    Stream.mapEffect(parseBunJsonLine),
    Stream.mapEffect((value) => decodeResponse(value)),
    Stream.runForEach((response) => Queue.offer(child.responses, Exit.succeed(response))),
    Effect.matchCauseEffect({
      onFailure: (cause) =>
        Queue.offer(
          child.responses,
          Exit.fail(processError("ProtocolFailure", "Code Kernel response failed.", cause)),
        ),
      onSuccess: () =>
        Queue.offer(
          child.responses,
          Exit.fail(processError("ProcessExited", "Code Kernel process exited.", undefined)),
        ),
    }),
  );

const spawnChild = Effect.fn("CodeKernelProcess.spawn")(function* (
  config: CodeKernelProcessConfig,
  parentScope: Scope.Scope,
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
) {
  const scope = yield* Scope.fork(parentScope);
  const responses =
    yield* Queue.unbounded<Exit.Exit<CodeKernelProcessResponse, CodeKernelProcessError>>();
  const handle = yield* ChildProcess.make("bun", ["run", config.entryPath], {
    detached: true,
    stdin: { stream: "pipe", endOnDone: false },
    stdout: "pipe",
    stderr: "ignore",
  }).pipe(
    Effect.provideService(Scope.Scope, scope),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.mapError((cause) =>
      processError("ProcessExited", "Code Kernel process did not start.", cause),
    ),
  );
  const child = { scope, handle, responses };
  yield* Effect.forkIn(listenForResponses(child), scope);
  const ready = yield* Queue.take(child.responses).pipe(
    Effect.flatMap((result) => result),
    Effect.timeoutOrElse({
      duration: config.startupTimeout ?? "10 seconds",
      orElse: () =>
        Effect.fail(
          processError("TimedOut", "Code Kernel process did not become ready.", undefined),
        ),
    }),
    Effect.exit,
  );
  if (Exit.isFailure(ready)) {
    yield* Scope.close(scope, ready);
    return yield* Effect.failCause(ready.cause);
  }
  if (!CodeKernelProcessResponse.guards.Ready(ready.value)) {
    yield* Scope.close(scope, Exit.void);
    return yield* processError(
      "ProtocolFailure",
      "Code Kernel process did not send a ready frame.",
      ready.value,
    );
  }
  return child;
});

const sendRequest = Effect.fn("CodeKernelProcess.send")(function* (
  child: KernelChild,
  request: CodeKernelProcessRequest,
  timeout: Duration.Input,
) {
  const encoded = yield* encodeRequest(request).pipe(
    Effect.mapError((cause) =>
      processError("ProtocolFailure", "Code Kernel request encoding failed.", cause),
    ),
  );
  yield* Stream.succeed(`${encoded}\n`).pipe(
    Stream.encodeText,
    Stream.run(child.handle.stdin),
    Effect.mapError((cause) =>
      processError("ProcessExited", "Code Kernel request write failed.", cause),
    ),
  );
  const response = yield* Queue.take(child.responses).pipe(
    Effect.flatMap((result) => result),
    Effect.timeoutOrElse({
      duration: timeout,
      orElse: () =>
        Effect.fail(
          processError("TimedOut", "Code Kernel cell exceeded its execution limit.", undefined),
        ),
    }),
  );
  if (CodeKernelProcessResponse.guards.Ready(response)) {
    return yield* processError(
      "ProtocolFailure",
      "Code Kernel sent an unexpected ready frame.",
      response,
    );
  }
  if (response.requestId !== request.requestId) {
    return yield* processError(
      "ProtocolFailure",
      `Expected response ${request.requestId}, received ${response.requestId}.`,
      response,
    );
  }
  return response;
});

interface KernelSupervisorState {
  child: KernelChild | undefined;
  nextRequestId: number;
  readonly parentScope: Scope.Scope;
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
}

const getChild = Effect.fn("CodeKernelProcess.getChild")(function* (
  config: CodeKernelProcessConfig,
  state: KernelSupervisorState,
) {
  if (state.child !== undefined) return state.child;
  state.child = yield* spawnChild(config, state.parentScope, state.spawner);
  return state.child;
});

const replaceChild = Effect.fn("CodeKernelProcess.replace")(function* (
  state: KernelSupervisorState,
) {
  if (state.child === undefined) return;
  yield* Scope.close(state.child.scope, Exit.void);
  state.child = undefined;
});

const takeRequestId = (state: KernelSupervisorState) =>
  Effect.sync(() => {
    state.nextRequestId += 1;
    return state.nextRequestId;
  });

const makeEvaluate = (config: CodeKernelProcessConfig, state: KernelSupervisorState) => {
  const evaluate = Effect.fn("CodeKernel.evaluate")(function* (input: EvaluateCellInput) {
    const active = yield* getChild(config, state);
    const requestId = yield* takeRequestId(state);
    const response = yield* sendRequest(
      active,
      CodeKernelProcessRequest.cases.Evaluate.make({
        requestId,
        cellId: input.cellId,
        source: input.source,
      }),
      config.cellTimeout ?? "30 seconds",
    );
    if (CodeKernelProcessResponse.guards.EvaluationSucceeded(response)) {
      return response.evaluation;
    }
    if (CodeKernelProcessResponse.guards.EvaluationFailed(response)) {
      return yield* response.error;
    }
    return yield* new CellInterruptedError({
      cellId: input.cellId,
      reason: "ProtocolFailure",
      message: "Code Kernel returned a reset response for a Cell.",
    });
  });

  return (input: EvaluateCellInput) =>
    evaluate(input).pipe(
      Effect.catchTag("CodeKernelProcessError", (error) =>
        replaceChild(state).pipe(
          Effect.andThen(
            Effect.fail(
              new CellInterruptedError({
                cellId: input.cellId,
                reason: error.reason,
                message: error.message,
              }),
            ),
          ),
        ),
      ),
    );
};

const makeReset = (config: CodeKernelProcessConfig, state: KernelSupervisorState) =>
  Effect.fn("CodeKernel.reset")(function* () {
    if (state.child === undefined) return;
    const active = state.child;
    const requestId = yield* takeRequestId(state);
    const result = yield* sendRequest(
      active,
      CodeKernelProcessRequest.cases.Reset.make({ requestId }),
      config.cellTimeout ?? "30 seconds",
    ).pipe(Effect.exit);
    if (Exit.isFailure(result)) {
      yield* replaceChild(state);
    }
  });

export const makeCodeKernel = (
  config: CodeKernelProcessConfig,
): Effect.Effect<CodeKernelShape, never, ChildProcessSpawner.ChildProcessSpawner | Scope.Scope> =>
  Effect.gen(function* () {
    const parentScope = yield* Scope.Scope;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const semaphore = yield* Semaphore.make(1);
    const state: KernelSupervisorState = {
      child: undefined,
      nextRequestId: 0,
      parentScope,
      spawner,
    };
    const evaluate = makeEvaluate(config, state);
    const reset = makeReset(config, state);

    return CodeKernel.of({
      evaluate: (input) => Semaphore.withPermit(semaphore, evaluate(input)),
      reset: Semaphore.withPermit(semaphore, reset()),
    });
  });

export const layerCodeKernel = (
  config: CodeKernelProcessConfig,
): Layer.Layer<CodeKernel, never, ChildProcessSpawner.ChildProcessSpawner> =>
  Layer.effect(CodeKernel, makeCodeKernel(config));
