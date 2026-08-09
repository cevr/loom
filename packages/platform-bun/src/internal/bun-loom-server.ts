import { LoomRpcs, maximumFrameSize } from "@cvr/loom-protocol";
import { BunSocketServer } from "@effect/platform-bun";
import { Layer } from "effect";
import type { SocketServer } from "effect/unstable/socket";
import { Rpc, RpcGroup, RpcSerialization, RpcServer } from "effect/unstable/rpc";

export interface BunLoomServerConfig {
  readonly socketPath: string;
}

export const layerBunLoomServer = (
  config: BunLoomServerConfig,
): Layer.Layer<
  never,
  SocketServer.SocketServerError,
  Rpc.ToHandler<RpcGroup.Rpcs<typeof LoomRpcs>>
> => {
  const protocol = RpcServer.layerProtocolSocketServer.pipe(
    Layer.provide(BunSocketServer.layer({ path: config.socketPath })),
    Layer.provide(RpcSerialization.layerNdjsonWith({ maxBufferSize: maximumFrameSize })),
  );
  return RpcServer.layer(LoomRpcs).pipe(Layer.provide(protocol));
};
