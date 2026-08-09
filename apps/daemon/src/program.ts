import {
  layerBunLoomServer,
  layerCellJournal,
  layerCodeKernelFactory,
  layerJobRecovery,
  prepareDaemonSocket,
} from "@cvr/loom-platform-bun";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { JobReconciler, layerAgentActor, layerConnectionHandshake } from "@cvr/loom-runtime";
import { Clock, Crypto, Effect, FileSystem, Layer, Path } from "effect";
import { SingleRunner } from "effect/unstable/cluster";
import { ChildProcessSpawner } from "effect/unstable/process";
import { loadDaemonConfig } from "./daemon-config.js";
import { layerLoomRpcHandlers } from "./rpc-handlers.js";

export const startupMessage = Effect.succeed("Loom daemon is ready");
const codeKernelEntry = new URL("../../code-kernel/src/main.ts", import.meta.url).pathname;

const reconcileJobs = Effect.fn("LoomDaemon.reconcileJobs")(function* (filename: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fs.makeDirectory(path.dirname(filename), { recursive: true });
  const results = yield* Effect.gen(function* () {
    const reconciler = yield* JobReconciler;
    return yield* reconciler.reconcile;
  }).pipe(Effect.provide(layerJobRecovery({ filename })));
  yield* Effect.logInfo("Job restart reconciliation completed.", results);
});

export const program: Effect.Effect<
  void,
  unknown,
  FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner | Crypto.Crypto
> = Effect.gen(function* () {
  const config = yield* loadDaemonConfig;
  yield* prepareDaemonSocket(config.socketPath);
  yield* reconcileJobs(config.databasePath);
  const daemonStartedAtMillis = yield* Clock.currentTimeMillis;
  const cluster = SingleRunner.layer({ runnerStorage: "memory" }).pipe(
    Layer.provide(SqliteClient.layer({ filename: config.databasePath })),
  );
  const agents = layerAgentActor.pipe(
    Layer.provide([
      cluster,
      layerCellJournal({ filename: config.databasePath }),
      layerCodeKernelFactory({ entryPath: codeKernelEntry }),
    ]),
  );
  const handlers = layerLoomRpcHandlers.pipe(
    Layer.provide(agents),
    Layer.provide(
      layerConnectionHandshake({
        workspaceRoot: config.workspaceRoot,
        daemonStartedAtMillis,
      }),
    ),
  );
  const server = layerBunLoomServer({ socketPath: config.socketPath }).pipe(
    Layer.provide(handlers),
    Layer.tap(() => startupMessage.pipe(Effect.flatMap(Effect.logInfo))),
  );
  return yield* Layer.launch(server);
});
