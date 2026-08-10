import { type DaemonUnavailableError, LoomClient, type LoomClientShape } from "@cvr/loom-client";
import { WorkspaceRoot } from "@cvr/loom-domain";
import { layerNodeLoomClient, layerNodeServices, startNodeDaemon } from "@cvr/loom-platform-node";
import { type Duration, Effect, FileSystem, Path } from "effect";

const daemonEntry = new URL("../../../../apps/daemon/src/main.ts", import.meta.url).pathname;

const connect = (
  workspaceRoot: WorkspaceRoot,
  socketPath: string,
  connectionTimeout: Duration.Input,
) =>
  Effect.gen(function* () {
    const client = yield* LoomClient;
    return yield* client.handshake;
  }).pipe(Effect.provide(layerNodeLoomClient({ workspaceRoot, socketPath, connectionTimeout })));

const resolveWorkspaceRoot = (cwd: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    return WorkspaceRoot.make(yield* fs.realPath(path.resolve(cwd)));
  });

const startOnUnavailable = <A, E>(
  workspaceRoot: WorkspaceRoot,
  attempt: Effect.Effect<A, E | DaemonUnavailableError>,
  retry: Effect.Effect<A, E | DaemonUnavailableError>,
) =>
  attempt.pipe(
    Effect.map((value) => ({ started: false, value })),
    Effect.catchTag("DaemonUnavailableError", () =>
      startNodeDaemon({ entryPath: daemonEntry, workspaceRoot }).pipe(
        Effect.flatMap(() => retry),
        Effect.map((value) => ({ started: true, value })),
      ),
    ),
  );

const ensureAt = Effect.fn("LoomPiExtension.ensureAt")(function* (workspaceRoot: WorkspaceRoot) {
  const socketPath = `${workspaceRoot}/.loom/daemon.sock`;
  const { started, value } = yield* startOnUnavailable(
    workspaceRoot,
    connect(workspaceRoot, socketPath, "100 millis"),
    connect(workspaceRoot, socketPath, "5 seconds"),
  );
  return { started, protocolVersion: value.protocolVersion, socketPath };
});

export interface LoomDaemonStatus {
  readonly started: boolean;
  readonly protocolVersion: number;
  readonly socketPath: string;
}

export const ensureLoomDaemon = (cwd: string) =>
  Effect.flatMap(resolveWorkspaceRoot(cwd), ensureAt).pipe(Effect.provide(layerNodeServices));

export type EnsureLoomDaemon = typeof ensureLoomDaemon;

export const runWithLoomClient = <A, E>(
  cwd: string,
  requestTimeout: Duration.Input,
  operation: (client: LoomClientShape) => Effect.Effect<A, E | DaemonUnavailableError>,
) =>
  Effect.gen(function* () {
    const workspaceRoot = yield* resolveWorkspaceRoot(cwd);
    return yield* Effect.gen(function* () {
      const client = yield* LoomClient;
      const { value } = yield* startOnUnavailable(
        workspaceRoot,
        operation(client),
        operation(client),
      );
      return value;
    }).pipe(
      Effect.provide(
        layerNodeLoomClient({
          workspaceRoot,
          socketPath: `${workspaceRoot}/.loom/daemon.sock`,
          connectionTimeout: "5 seconds",
          requestTimeout,
        }),
      ),
    );
  }).pipe(Effect.provide(layerNodeServices));
