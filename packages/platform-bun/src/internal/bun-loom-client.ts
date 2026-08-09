import {
  DaemonUnavailableError,
  LoomClient,
  MessageTooLargeError,
  type LoomClientShape,
} from "@cvr/loom-client";
import type { AgentOwner, WorkspaceRoot } from "@cvr/loom-domain";
import {
  currentProtocolVersion,
  LoomRpcs,
  maximumCellSourceLength,
  maximumFrameSize,
  minimumProtocolVersion,
  type EvaluateCellRequest,
} from "@cvr/loom-protocol";
import { BunSocket } from "@effect/platform-bun";
import { Duration, Effect, Layer, Schedule, Scope } from "effect";
import { RpcClient, RpcClientError, RpcSerialization } from "effect/unstable/rpc";
import type { Socket } from "effect/unstable/socket";

export interface BunLoomClientConfig {
  readonly socketPath: string;
  readonly workspaceRoot: WorkspaceRoot;
  readonly connectionTimeout?: Duration.Input;
}

const unavailable = (config: BunLoomClientConfig, operation: string, cause: unknown) =>
  new DaemonUnavailableError({ socketPath: config.socketPath, operation, cause });

type LoomRpcClient = RpcClient.FromGroup<typeof LoomRpcs, RpcClientError.RpcClientError>;

const withTimeout = <A, E, R>(
  config: BunLoomClientConfig,
  operation: string,
  effect: Effect.Effect<A, E, R>,
) =>
  effect.pipe(
    Effect.timeoutOrElse({
      duration: config.connectionTimeout ?? "1 second",
      orElse: () => Effect.fail(unavailable(config, operation, "connection timed out")),
    }),
  );

const makeHandshake = (config: BunLoomClientConfig, rpc: LoomRpcClient) =>
  Effect.fn("BunLoomClient.handshake")(function* () {
    return yield* rpc["Connection.Handshake"]({
      workspaceRoot: config.workspaceRoot,
      minimumProtocolVersion,
      maximumProtocolVersion: currentProtocolVersion,
    });
  });

const makeRunHandshake = (config: BunLoomClientConfig, rpc: LoomRpcClient) => {
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
  config: BunLoomClientConfig,
  rpc: LoomRpcClient,
  runHandshake: LoomClientShape["handshake"],
) =>
  Effect.fn("BunLoomClient.evaluateCell")(function* (request: EvaluateCellRequest) {
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
  config: BunLoomClientConfig,
  rpc: LoomRpcClient,
  runHandshake: LoomClientShape["handshake"],
) =>
  Effect.fn("BunLoomClient.resetCodeKernel")(function* (owner: AgentOwner) {
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

const makeBunLoomClient = (
  config: BunLoomClientConfig,
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

export const layerBunLoomClient = (
  config: BunLoomClientConfig,
): Layer.Layer<LoomClient, Socket.SocketError> => {
  const protocol = RpcClient.layerProtocolSocket({ retryTransientErrors: true }).pipe(
    Layer.provide(BunSocket.layerNet({ path: config.socketPath })),
    Layer.provide(RpcSerialization.layerNdjsonWith({ maxBufferSize: maximumFrameSize })),
  );
  return Layer.effect(LoomClient, makeBunLoomClient(config)).pipe(Layer.provide(protocol));
};
