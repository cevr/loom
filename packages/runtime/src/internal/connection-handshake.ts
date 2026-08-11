import type { WorkspaceRoot } from "@cvr/loom-domain";
import {
  HandshakeSuccess,
  IncompatibleProtocolError,
  maximumFrameSize,
  maximumProtocolVersion,
  minimumProtocolVersion,
  WorkspaceMismatchError,
  type HandshakeError,
  type HandshakeRequest,
} from "@cvr/loom-protocol";
import { Context, Duration, Effect, Layer } from "effect";

export interface ConnectionHandshakeConfig {
  readonly workspaceRoot: WorkspaceRoot;
  readonly daemonStartedAtMillis: number;
  readonly codeKernelIdleLease: Duration.Input;
  readonly maximumFrameSize?: number;
}

export interface ConnectionHandshakeShape {
  readonly handshake: (
    request: HandshakeRequest,
  ) => Effect.Effect<HandshakeSuccess, HandshakeError>;
}

export class ConnectionHandshake extends Context.Service<
  ConnectionHandshake,
  ConnectionHandshakeShape
>()("@cvr/loom-runtime/ConnectionHandshake") {}

const negotiateVersion = (request: HandshakeRequest) => {
  const lowestCommon = Math.max(request.minimumProtocolVersion, minimumProtocolVersion);
  const highestCommon = Math.min(request.maximumProtocolVersion, maximumProtocolVersion);
  if (lowestCommon <= highestCommon) return Effect.succeed(highestCommon);
  return Effect.fail(
    new IncompatibleProtocolError({
      clientMinimum: request.minimumProtocolVersion,
      clientMaximum: request.maximumProtocolVersion,
      daemonMinimum: minimumProtocolVersion,
      daemonMaximum: maximumProtocolVersion,
    }),
  );
};

export const makeConnectionHandshake = (
  config: ConnectionHandshakeConfig,
): ConnectionHandshakeShape => {
  const handshake = Effect.fn("ConnectionHandshake.handshake")(function* (
    request: HandshakeRequest,
  ) {
    if (request.workspaceRoot !== config.workspaceRoot) {
      return yield* new WorkspaceMismatchError({
        expected: config.workspaceRoot,
        received: request.workspaceRoot,
      });
    }
    return HandshakeSuccess.make({
      workspaceRoot: config.workspaceRoot,
      protocolVersion: yield* negotiateVersion(request),
      maximumFrameSize: config.maximumFrameSize ?? maximumFrameSize,
      daemonStartedAtMillis: config.daemonStartedAtMillis,
      codeKernelIdleLeaseMillis: Duration.toMillis(config.codeKernelIdleLease),
    });
  });
  return ConnectionHandshake.of({ handshake });
};

export const layerConnectionHandshake = (
  config: ConnectionHandshakeConfig,
): Layer.Layer<ConnectionHandshake> =>
  Layer.succeed(ConnectionHandshake, makeConnectionHandshake(config));
