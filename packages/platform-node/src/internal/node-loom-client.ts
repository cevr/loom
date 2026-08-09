import { layerLoomRpcClient, LoomClient, type LoomRpcClientConfig } from "@cvr/loom-client";
import { maximumFrameSize } from "@cvr/loom-protocol";
import { NodeSocket } from "@effect/platform-node-shared";
import { Layer } from "effect";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";
import type { Socket } from "effect/unstable/socket";

export type NodeLoomClientConfig = LoomRpcClientConfig;

export const layerNodeLoomClient = (
  config: NodeLoomClientConfig,
): Layer.Layer<LoomClient, Socket.SocketError> => {
  const protocol = RpcClient.layerProtocolSocket({ retryTransientErrors: true }).pipe(
    Layer.provide(NodeSocket.layerNet({ path: config.socketPath })),
    Layer.provide(RpcSerialization.layerNdjsonWith({ maxBufferSize: maximumFrameSize })),
  );
  return layerLoomRpcClient(config).pipe(Layer.provide(protocol));
};
