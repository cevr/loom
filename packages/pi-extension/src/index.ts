import { LoomClient } from "@cvr/loom-client";
import { WorkspaceRoot } from "@cvr/loom-domain";
import { layerNodeLoomClient, layerNodeServices, startNodeDaemon } from "@cvr/loom-platform-node";
import type {
  ExtensionAPI,
  ExtensionContext,
  SessionStartEvent,
} from "@earendil-works/pi-coding-agent";
import { Duration, Effect, FileSystem, Path } from "effect";

export interface LoomDaemonStatus {
  readonly started: boolean;
  readonly protocolVersion: number;
  readonly socketPath: string;
}

export type EnsureLoomDaemon = (cwd: string) => Effect.Effect<LoomDaemonStatus, unknown>;

export interface LoomExtensionApi {
  readonly on: (
    event: "session_start",
    handler: (event: SessionStartEvent, context: ExtensionContext) => Promise<void> | void,
  ) => void;
  readonly registerCommand: ExtensionAPI["registerCommand"];
}

const daemonEntry = new URL("../../../apps/daemon/src/main.ts", import.meta.url).pathname;

const connect = (
  workspaceRoot: WorkspaceRoot,
  socketPath: string,
  connectionTimeout: Duration.Input,
) =>
  Effect.gen(function* () {
    const client = yield* LoomClient;
    return yield* client.handshake;
  }).pipe(
    Effect.provide(
      layerNodeLoomClient({
        workspaceRoot,
        socketPath,
        connectionTimeout,
      }),
    ),
  );

const startDaemon = Effect.fn("LoomPiExtension.startDaemon")(function* (
  workspaceRoot: WorkspaceRoot,
) {
  yield* startNodeDaemon({ entryPath: daemonEntry, workspaceRoot });
});

export const ensureLoomDaemon: EnsureLoomDaemon = (cwd) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const workspaceRoot = WorkspaceRoot.make(yield* fs.realPath(path.resolve(cwd)));
    const socketPath = `${workspaceRoot}/.loom/daemon.sock`;
    return yield* connect(workspaceRoot, socketPath, "100 millis").pipe(
      Effect.map((handshake) => ({
        started: false,
        protocolVersion: handshake.protocolVersion,
        socketPath,
      })),
      Effect.catchTag("DaemonUnavailableError", () =>
        startDaemon(workspaceRoot).pipe(
          Effect.flatMap(() => connect(workspaceRoot, socketPath, "5 seconds")),
          Effect.map((handshake) => ({
            started: true,
            protocolVersion: handshake.protocolVersion,
            socketPath,
          })),
        ),
      ),
    );
  }).pipe(Effect.provide(layerNodeServices));

const notifyFailure = (context: ExtensionContext, cause: unknown) =>
  Effect.sync(() => context.ui.notify(`Loom daemon failed: ${String(cause)}`, "error"));

const ensureForSession = (context: ExtensionContext, ensureDaemon: EnsureLoomDaemon) =>
  ensureDaemon(context.cwd).pipe(
    Effect.matchEffect({
      onFailure: (cause) => notifyFailure(context, cause),
      onSuccess: (status) => {
        if (!status.started) return Effect.void;
        return Effect.sync(() => context.ui.notify("Loom daemon started.", "info"));
      },
    }),
    Effect.runPromise,
  );

export const registerLoomExtension = (
  pi: LoomExtensionApi,
  ensureDaemon: EnsureLoomDaemon = ensureLoomDaemon,
): void => {
  pi.on("session_start", (_event, context) => ensureForSession(context, ensureDaemon));
  pi.registerCommand("loom", {
    description: "Show the Loom daemon state",
    handler: (_arguments, context) =>
      ensureDaemon(context.cwd).pipe(
        Effect.matchEffect({
          onFailure: (cause) => notifyFailure(context, cause),
          onSuccess: (status) =>
            Effect.sync(() =>
              context.ui.notify(
                `Loom daemon ready. Protocol ${status.protocolVersion}. Socket ${status.socketPath}`,
                "info",
              ),
            ),
        }),
        Effect.runPromise,
      ),
  });
};

export default function loomExtension(pi: LoomExtensionApi): void {
  registerLoomExtension(pi);
}
