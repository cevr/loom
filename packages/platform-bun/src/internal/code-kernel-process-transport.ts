import { CodeKernelProcessRequest, CodeKernelProcessResponse } from "@cvr/loom-protocol";
import {
  Cause,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Option,
  Queue,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import type { ChildProcessHandle } from "effect/unstable/process/ChildProcessSpawner";
import type { PlatformError } from "effect/PlatformError";
import {
  captureKernelStderr,
  diagnosticFor,
  failWithDiagnostic,
  type CodeKernelDiagnosticsConfig,
  type KernelDiagnosticSource,
} from "./code-kernel-diagnostics.js";
import type { KernelDiagnosticFile } from "./code-kernel-diagnostic-store.js";
import { CodeKernelProcessError } from "./code-kernel-process-error.js";
import {
  closeKernelProcessScope,
  type KernelProcessLifecycle,
  type KernelProcessRegistration,
  makeKernelProcessRegistration,
  registerKernelProcess,
  releaseKernelProcess,
} from "./code-kernel-process-lifecycle.js";

export interface CodeKernelProcessTransportConfig extends CodeKernelDiagnosticsConfig {
  readonly entryPath: string;
  readonly executable?: string;
  readonly startupTimeout?: Duration.Input;
  readonly forceKillAfter?: Duration.Input;
}

export interface KernelChild {
  readonly scope: Scope.Closeable;
  readonly handle: ChildProcessHandle;
  readonly responses: Queue.Queue<CodeKernelProcessResponse, CodeKernelProcessError>;
  readonly diagnostics: KernelDiagnosticSource;
  readonly stderrCapture: Fiber.Fiber<void>;
  readonly registration: KernelProcessRegistration;
}

export type ReserveKernelDiagnostic = (
  pid: number,
) => Effect.Effect<Option.Option<KernelDiagnosticFile>, PlatformError, Scope.Scope>;

const decodeResponse = Schema.decodeEffect(Schema.fromJsonString(CodeKernelProcessResponse));
const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(CodeKernelProcessRequest));

const failResponses = (
  child: KernelChild,
  error: {
    readonly reason: CodeKernelProcessError["reason"];
    readonly message: string;
    readonly cause: CodeKernelProcessError["cause"];
    readonly exitCode?: number;
  },
) =>
  diagnosticFor(child.diagnostics, { exitCode: error.exitCode }).pipe(
    Effect.flatMap((diagnostic) =>
      Queue.fail(
        child.responses,
        new CodeKernelProcessError({
          reason: error.reason,
          message: error.message,
          cause: error.cause,
          diagnostic,
        }),
      ),
    ),
  );

const listenForResponses = (child: KernelChild) =>
  child.handle.stdout.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.filter((line) => line.length > 0),
    Stream.mapEffect((line) => decodeResponse(line)),
    Stream.runForEach((response) => Queue.offer(child.responses, response)),
    Effect.matchEffect({
      onFailure: (cause) =>
        failResponses(child, {
          reason: "ProtocolFailure",
          message: "Code Kernel response failed.",
          cause,
        }),
      onSuccess: () => reportProcessExit(child),
    }),
  );

export const releaseKernelChild = (child: KernelChild) => releaseKernelProcess(child.registration);

const reportProcessExit = Effect.fn("CodeKernelProcess.reportExit")(function* (child: KernelChild) {
  yield* Fiber.await(child.stderrCapture);
  yield* child.handle.exitCode.pipe(
    Effect.matchEffect({
      onFailure: (cause) =>
        failResponses(child, {
          reason: "ProcessExited",
          message: "Code Kernel process exit status was not available.",
          cause,
        }),
      onSuccess: (exitCode) =>
        failResponses(child, {
          reason: "ProcessExited",
          message: `Code Kernel process exited with code ${exitCode}.`,
          cause: Cause.empty,
          exitCode,
        }),
    }),
    Effect.ensuring(releaseKernelChild(child)),
  );
});

const makeChildHandle = (
  config: CodeKernelProcessTransportConfig,
  scope: Scope.Scope,
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
) =>
  ChildProcess.make(config.executable ?? "bun", ["run", config.entryPath], {
    stdin: { stream: "pipe", endOnDone: false },
    stdout: "pipe",
    stderr: "pipe",
    killSignal: "SIGTERM",
    forceKillAfter: config.forceKillAfter ?? "1 second",
  }).pipe(
    Effect.provideService(Scope.Scope, scope),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.mapError(
      (cause) =>
        new CodeKernelProcessError({
          reason: "ProcessExited",
          message: "Code Kernel process did not start.",
          cause,
        }),
    ),
  );

const waitForReady = Effect.fn("CodeKernelProcess.waitForReady")(function* (
  config: CodeKernelProcessTransportConfig,
  child: KernelChild,
) {
  const ready = yield* Queue.take(child.responses).pipe(
    Effect.timeoutOrElse({
      duration: config.startupTimeout ?? "10 seconds",
      orElse: () =>
        failWithDiagnostic(
          child.diagnostics,
          "TimedOut",
          "Code Kernel process did not become ready.",
          Cause.empty,
        ),
    }),
  );
  if (CodeKernelProcessResponse.guards.Ready(ready)) return;
  return yield* failWithDiagnostic(
    child.diagnostics,
    "ProtocolFailure",
    "Code Kernel process did not send a ready frame.",
    ready,
  );
});

export const spawnKernelChild = Effect.fn("CodeKernelProcess.spawn")(function* (
  config: CodeKernelProcessTransportConfig,
  parentScope: Scope.Scope,
  spawner: ChildProcessSpawner.ChildProcessSpawner["Service"],
  fs: FileSystem.FileSystem,
  reserveDiagnostic: Option.Option<ReserveKernelDiagnostic>,
  lifecycle: Option.Option<KernelProcessLifecycle>,
) {
  const scope = yield* Scope.fork(parentScope);
  const registration = yield* makeKernelProcessRegistration(lifecycle);
  yield* Scope.addFinalizer(scope, releaseKernelProcess(registration));
  return yield* Effect.gen(function* () {
    const responses = yield* Queue.unbounded<CodeKernelProcessResponse, CodeKernelProcessError>();
    const stderrTail = yield* Ref.make("");
    const handle = yield* makeChildHandle(config, scope, spawner);
    let diagnosticFile = Option.none<KernelDiagnosticFile>();
    if (Option.isSome(reserveDiagnostic)) {
      diagnosticFile = yield* reserveDiagnostic.value(handle.pid).pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.tapError((error) =>
          Effect.logWarning("Code Kernel stderr file is unavailable.", error),
        ),
        Effect.orElseSucceed(() => Option.none()),
      );
    }
    const diagnostics = { stderrTail, file: diagnosticFile };
    const stderrCapture = yield* Effect.forkIn(
      captureKernelStderr(config, handle, diagnostics, fs).pipe(
        Effect.tapError((error) => Effect.logWarning("Code Kernel stderr capture failed.", error)),
        Effect.ignore,
      ),
      scope,
    );
    const child = {
      scope,
      handle,
      responses,
      diagnostics,
      stderrCapture,
      registration,
    };
    yield* Effect.forkIn(listenForResponses(child), scope);
    yield* waitForReady(config, child);
    yield* Effect.uninterruptible(registerKernelProcess(registration, handle.pid));
    return child;
  }).pipe(Effect.onError((cause) => closeKernelProcessScope(registration, scope, cause)));
});

const writeRequest = Effect.fn("CodeKernelProcess.writeRequest")(function* (
  child: KernelChild,
  request: CodeKernelProcessRequest,
) {
  const encoded = yield* encodeRequest(request).pipe(
    Effect.catch((cause) =>
      failWithDiagnostic(
        child.diagnostics,
        "ProtocolFailure",
        "Code Kernel request encoding failed.",
        cause,
        request.requestId,
      ),
    ),
  );
  yield* Stream.succeed(`${encoded}\n`).pipe(
    Stream.encodeText,
    Stream.run(child.handle.stdin),
    Effect.catch((cause) =>
      failWithDiagnostic(
        child.diagnostics,
        "ProcessExited",
        "Code Kernel request write failed.",
        cause,
        request.requestId,
      ),
    ),
  );
});

export const sendKernelRequest = Effect.fn("CodeKernelProcess.send")(function* (
  child: KernelChild,
  request: CodeKernelProcessRequest,
  timeout: Duration.Input,
) {
  let timeoutMessage = "Code Kernel reset exceeded its execution limit.";
  if (CodeKernelProcessRequest.guards.Evaluate(request)) {
    timeoutMessage = "Code Kernel Cell exceeded its execution limit.";
  }
  yield* writeRequest(child, request);
  const response = yield* Queue.take(child.responses).pipe(
    Effect.timeoutOrElse({
      duration: timeout,
      orElse: () =>
        failWithDiagnostic(
          child.diagnostics,
          "TimedOut",
          timeoutMessage,
          Cause.empty,
          request.requestId,
        ),
    }),
  );
  if (CodeKernelProcessResponse.guards.Ready(response)) {
    return yield* failWithDiagnostic(
      child.diagnostics,
      "ProtocolFailure",
      "Code Kernel sent an unexpected ready frame.",
      response,
      request.requestId,
    );
  }
  if (response.requestId === request.requestId) return response;
  return yield* failWithDiagnostic(
    child.diagnostics,
    "ProtocolFailure",
    `Expected response ${request.requestId}, received ${response.requestId}.`,
    response,
    request.requestId,
  );
});
