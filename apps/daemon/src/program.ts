import {
  layerBunLoomServer,
  layerCodeKernelFactory,
  layerJobRecovery,
  layerLoomSqlite,
  layerSqliteCellJournal,
  prepareDaemonSocket,
} from "@cvr/loom-platform-bun";
import { JobReconciler, layerAgentActor, layerConnectionHandshake } from "@cvr/loom-runtime";
import { Clock, Effect, FileSystem, Layer, Path } from "effect";
import { SingleRunner } from "effect/unstable/cluster";
import { loadDaemonConfig } from "./daemon-config.js";
import { layerLoomRpcHandlers } from "./rpc-handlers.js";

export const startupMessage = Effect.succeed("Loom daemon is ready");
const codeKernelEntry = new URL("../../code-kernel/src/main.ts", import.meta.url).pathname;

const reconcileJobs = Effect.fn("LoomDaemon.reconcileJobs")(function* () {
  const results = yield* Effect.gen(function* () {
    const reconciler = yield* JobReconciler;
    return yield* reconciler.reconcile;
  }).pipe(Effect.provide(layerJobRecovery));
  yield* Effect.logInfo("Job restart reconciliation completed.", results);
});

export const program = Effect.gen(function* () {
  const config = yield* loadDaemonConfig;
  yield* prepareDaemonSocket(config.socketPath);
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fs.makeDirectory(path.dirname(config.databasePath), { recursive: true });
  return yield* Effect.gen(function* () {
    yield* reconcileJobs();
    const daemonStartedAtMillis = yield* Clock.currentTimeMillis;
    const agents = layerAgentActor.pipe(
      Layer.provide([
        SingleRunner.layer({ runnerStorage: "memory" }),
        layerSqliteCellJournal,
        layerCodeKernelFactory({
          entryPath: codeKernelEntry,
          diagnosticsDirectory: `${config.workspaceRoot}/.loom/diagnostics/code-kernels`,
        }),
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
  }).pipe(Effect.provide(layerLoomSqlite({ filename: config.databasePath })));
});
