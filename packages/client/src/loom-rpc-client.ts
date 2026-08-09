import type { AgentOwner, WorkspaceRoot } from "@cvr/loom-domain";
import {
  currentProtocolVersion,
  LoomRpcs,
  maximumCellSourceLength,
  minimumProtocolVersion,
  type EvaluateCellRequest,
} from "@cvr/loom-protocol";
import { Duration, Effect, Layer, Schedule, Scope } from "effect";
import { RpcClient, RpcClientError } from "effect/unstable/rpc";
import { DaemonUnavailableError } from "./daemon-unavailable-error.js";
import { LoomClient, type LoomClientShape } from "./loom-client.js";
import { MessageTooLargeError } from "./message-too-large-error.js";

export interface LoomRpcClientConfig {
  readonly socketPath: string;
  readonly workspaceRoot: WorkspaceRoot;
  readonly connectionTimeout?: Duration.Input;
}

const unavailable = (config: LoomRpcClientConfig, operation: string, cause: unknown) =>
  new DaemonUnavailableError({ socketPath: config.socketPath, operation, cause });

type RpcClientShape = RpcClient.FromGroup<typeof LoomRpcs, RpcClientError.RpcClientError>;

const withTimeout = <A, E, R>(
  config: LoomRpcClientConfig,
  operation: string,
  effect: Effect.Effect<A, E, R>,
) =>
  effect.pipe(
    Effect.timeoutOrElse({
      duration: config.connectionTimeout ?? "1 second",
      orElse: () => Effect.fail(unavailable(config, operation, "connection timed out")),
    }),
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
    handshake().pipe(
      Effect.catchTag("RpcClientError", (cause) =>
        Effect.fail(unavailable(config, "handshake", cause)),
      ),
      Effect.retry({
        while: (error) => error instanceof DaemonUnavailableError,
        schedule: Schedule.spaced("25 millis"),
      }),
    ),
  );
};

const makeEvaluateCell = (
  config: LoomRpcClientConfig,
  rpc: RpcClientShape,
  runHandshake: LoomClientShape["handshake"],
) =>
  Effect.fn("LoomRpcClient.evaluateCell")(function* (request: EvaluateCellRequest) {
    if (request.source.length > maximumCellSourceLength) {
      return yield* new MessageTooLargeError({
        operation: "evaluateCell",
        size: request.source.length,
        maximum: maximumCellSourceLength,
      });
    }
    return yield* withTimeout(
      config,
      "evaluateCell",
      runHandshake.pipe(
        Effect.flatMap(() => rpc["CodeKernel.EvaluateCell"](request)),
        Effect.catchTag("RpcClientError", (cause) =>
          Effect.fail(unavailable(config, "evaluateCell", cause)),
        ),
      ),
    );
  });

const makeResetCodeKernel = (
  config: LoomRpcClientConfig,
  rpc: RpcClientShape,
  runHandshake: LoomClientShape["handshake"],
) =>
  Effect.fn("LoomRpcClient.resetCodeKernel")(function* (owner: AgentOwner) {
    return yield* withTimeout(
      config,
      "resetCodeKernel",
      runHandshake.pipe(
        Effect.flatMap(() => rpc["CodeKernel.Reset"](owner)),
        Effect.catchTag("RpcClientError", (cause) =>
          Effect.fail(unavailable(config, "resetCodeKernel", cause)),
        ),
      ),
    );
  });

const makeLoomRpcClient = (
  config: LoomRpcClientConfig,
): Effect.Effect<LoomClientShape, never, RpcClient.Protocol | Scope.Scope> =>
  Effect.gen(function* () {
    const rpc = yield* RpcClient.make(LoomRpcs);
    const runHandshake = makeRunHandshake(config, rpc);
    return LoomClient.of({
      handshake: runHandshake,
      evaluateCell: makeEvaluateCell(config, rpc, runHandshake),
      resetCodeKernel: makeResetCodeKernel(config, rpc, runHandshake),
    });
  });

export const layerLoomRpcClient = (
  config: LoomRpcClientConfig,
): Layer.Layer<LoomClient, never, RpcClient.Protocol> =>
  Layer.effect(LoomClient, makeLoomRpcClient(config));
