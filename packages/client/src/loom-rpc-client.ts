import type { WorkspaceRoot } from "@cvr/loom-domain";
import {
  currentProtocolVersion,
  LoomRpcs,
  maximumCellSourceLength,
  minimumProtocolVersion,
  type EvaluateCellRequest,
} from "@cvr/loom-protocol";
import { Duration, Effect, Layer, Option, Schedule, Scope } from "effect";
import { RpcClient, RpcClientError } from "effect/unstable/rpc";
import {
  DaemonUnavailableError,
  type DaemonUnavailableReason,
} from "./daemon-unavailable-error.js";
import { LoomClient, type LoomClientShape } from "./loom-client.js";
import { MessageTooLargeError } from "./message-too-large-error.js";

export interface LoomRpcClientConfig {
  readonly socketPath: string;
  readonly workspaceRoot: WorkspaceRoot;
  readonly connectionTimeout?: Duration.Input;
  readonly requestTimeout?: Duration.Input;
}

const unavailable = (
  config: LoomRpcClientConfig,
  operation: string,
  reason: DaemonUnavailableReason,
  cause: Option.Option<unknown>,
) => new DaemonUnavailableError({ socketPath: config.socketPath, operation, reason, cause });

type RpcClientShape = RpcClient.FromGroup<typeof LoomRpcs, RpcClientError.RpcClientError>;

const withTimeout = <A, E, R>(
  config: LoomRpcClientConfig,
  operation: string,
  reason: DaemonUnavailableReason,
  effect: Effect.Effect<A, E, R>,
  duration: Duration.Input,
) =>
  effect.pipe(
    Effect.timeoutOrElse({
      duration,
      orElse: () => Effect.fail(unavailable(config, operation, reason, Option.none())),
    }),
  );

const makeDaemonRequest = <Request, Success, Error, Requirements>(
  name: string,
  operation: string,
  config: LoomRpcClientConfig,
  runHandshake: LoomClientShape["handshake"],
  send: (
    request: Request,
  ) => Effect.Effect<Success, Error | RpcClientError.RpcClientError, Requirements>,
  timeoutFor?: (request: Request) => Duration.Input,
) =>
  Effect.fn(name)((request: Request) =>
    withTimeout(
      config,
      operation,
      "RequestTimeout",
      runHandshake.pipe(
        Effect.flatMap(() => send(request)),
        Effect.catchTag("RpcClientError", (cause) =>
          Effect.fail(unavailable(config, operation, "TransportFailure", Option.some(cause))),
        ),
      ),
      timeoutFor?.(request) ?? config.requestTimeout ?? "10 seconds",
    ),
  );

const makeHandshake = (config: LoomRpcClientConfig, rpc: RpcClientShape) =>
  Effect.fn("LoomRpcClient.handshake")(function* () {
    return yield* rpc["Connection.Handshake"]({
      workspaceRoot: config.workspaceRoot,
      minimumProtocolVersion,
      maximumProtocolVersion: currentProtocolVersion,
    });
  });

const makeRunHandshake = (config: LoomRpcClientConfig, rpc: RpcClientShape) => {
  const handshake = makeHandshake(config, rpc);
  return withTimeout(
    config,
    "handshake",
    "ConnectionTimeout",
    handshake().pipe(
      Effect.catchTag("RpcClientError", (cause) =>
        Effect.fail(unavailable(config, "handshake", "TransportFailure", Option.some(cause))),
      ),
      Effect.retry({
        while: (error) => error instanceof DaemonUnavailableError,
        schedule: Schedule.spaced("25 millis"),
      }),
    ),
    config.connectionTimeout ?? "1 second",
  );
};

const makeEvaluateCell = (
  config: LoomRpcClientConfig,
  rpc: RpcClientShape,
  runHandshake: LoomClientShape["handshake"],
) => {
  const send = makeDaemonRequest(
    "LoomRpcClient.sendEvaluateCell",
    "evaluateCell",
    config,
    runHandshake,
    rpc["CodeKernel.EvaluateCell"],
  );
  return Effect.fn("LoomRpcClient.evaluateCell")(function* (request: EvaluateCellRequest) {
    if (request.source.length > maximumCellSourceLength) {
      return yield* new MessageTooLargeError({
        operation: "evaluateCell",
        size: request.source.length,
        maximum: maximumCellSourceLength,
      });
    }
    return yield* send(request);
  });
};

const makeWorkflowControls = (
  config: LoomRpcClientConfig,
  rpc: RpcClientShape,
  runHandshake: LoomClientShape["handshake"],
) => ({
  inspectWorkflow: makeDaemonRequest(
    "LoomRpcClient.inspectWorkflow",
    "inspectWorkflow",
    config,
    runHandshake,
    rpc["Workflow.Inspect"],
  ),
  interruptWorkflow: makeDaemonRequest(
    "LoomRpcClient.interruptWorkflow",
    "interruptWorkflow",
    config,
    runHandshake,
    rpc["Workflow.Interrupt"],
  ),
  decideWorkflowCompensation: makeDaemonRequest(
    "LoomRpcClient.decideWorkflowCompensation",
    "decideWorkflowCompensation",
    config,
    runHandshake,
    rpc["Workflow.DecideCompensation"],
  ),
});

const makeJobReadControls = (
  config: LoomRpcClientConfig,
  rpc: RpcClientShape,
  runHandshake: LoomClientShape["handshake"],
) => ({
  inspectJob: makeDaemonRequest(
    "LoomRpcClient.inspectJob",
    "inspectJob",
    config,
    runHandshake,
    rpc["Job.Inspect"],
  ),
  readJobOutput: makeDaemonRequest(
    "LoomRpcClient.readJobOutput",
    "readJobOutput",
    config,
    runHandshake,
    rpc["Job.Output"],
  ),
});

const makeJobLifecycleControls = (
  config: LoomRpcClientConfig,
  rpc: RpcClientShape,
  runHandshake: LoomClientShape["handshake"],
) => {
  const leaseTimeout = (foregroundLeaseMillis: number) =>
    Duration.millis(
      foregroundLeaseMillis + Duration.toMillis(config.requestTimeout ?? "10 seconds"),
    );
  return {
    startJob: makeDaemonRequest(
      "LoomRpcClient.startJob",
      "startJob",
      config,
      runHandshake,
      rpc["Job.Start"],
      (request) => leaseTimeout(request.foregroundLeaseMillis),
    ),
    awaitJob: makeDaemonRequest(
      "LoomRpcClient.awaitJob",
      "awaitJob",
      config,
      runHandshake,
      rpc["Job.Await"],
      (request) => leaseTimeout(request.foregroundLeaseMillis),
    ),
    cancelJob: makeDaemonRequest(
      "LoomRpcClient.cancelJob",
      "cancelJob",
      config,
      runHandshake,
      rpc["Job.Cancel"],
    ),
    detachJob: makeDaemonRequest(
      "LoomRpcClient.detachJob",
      "detachJob",
      config,
      runHandshake,
      rpc["Job.Detach"],
    ),
  };
};

const makeJobControls = (
  config: LoomRpcClientConfig,
  rpc: RpcClientShape,
  runHandshake: LoomClientShape["handshake"],
) => ({
  ...makeJobLifecycleControls(config, rpc, runHandshake),
  ...makeJobReadControls(config, rpc, runHandshake),
});

const makeLoomRpcClient = (
  config: LoomRpcClientConfig,
): Effect.Effect<LoomClientShape, never, RpcClient.Protocol | Scope.Scope> =>
  Effect.gen(function* () {
    const rpc = yield* RpcClient.make(LoomRpcs);
    const runHandshake = makeRunHandshake(config, rpc);
    return LoomClient.of({
      ...makeJobControls(config, rpc, runHandshake),
      ...makeWorkflowControls(config, rpc, runHandshake),
      handshake: runHandshake,
      closeSession: makeDaemonRequest(
        "LoomRpcClient.closeSession",
        "closeSession",
        config,
        runHandshake,
        (sessionId) => rpc["Session.Close"]({ sessionId }),
      ),
      evaluateCell: makeEvaluateCell(config, rpc, runHandshake),
      resetCodeKernel: makeDaemonRequest(
        "LoomRpcClient.resetCodeKernel",
        "resetCodeKernel",
        config,
        runHandshake,
        rpc["CodeKernel.Reset"],
      ),
      startWorkflow: makeDaemonRequest(
        "LoomRpcClient.startWorkflow",
        "startWorkflow",
        config,
        runHandshake,
        rpc["Workflow.Start"],
      ),
      executeWorkflow: makeDaemonRequest(
        "LoomRpcClient.executeWorkflow",
        "executeWorkflow",
        config,
        runHandshake,
        rpc["Workflow.Execute"],
      ),
      signalWorkflow: makeDaemonRequest(
        "LoomRpcClient.signalWorkflow",
        "signalWorkflow",
        config,
        runHandshake,
        rpc["Workflow.Signal"],
      ),
    });
  });

export const layerLoomRpcClient = (
  config: LoomRpcClientConfig,
): Layer.Layer<LoomClient, never, RpcClient.Protocol> =>
  Layer.effect(LoomClient, makeLoomRpcClient(config));
