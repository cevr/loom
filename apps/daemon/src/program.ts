import {
  layerBunLoomServer,
  layerCodeKernelFactory,
  layerBunProcessInspector,
  layerJobRecovery,
  layerLoomSqlite,
  layerLoomWorkflowRuntime,
  layerSqliteJobProcessStore,
  layerSqliteWorkflowChildAgentStore,
  layerSqliteWorkflowJobStore,
  layerWorkflowCapabilities,
  layerSqliteCellJournal,
  prepareDaemonSocket,
} from "@cvr/loom-platform-bun";
import {
  JobReconciler,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  layerActorStateHub,
  layerAgentActor,
  layerConnectionHandshake,
} from "@cvr/loom-runtime";
import { Clock, Context, Effect, FileSystem, Layer, Path } from "effect";
import { SingleRunner } from "effect/unstable/cluster";
import { type DaemonConfig, loadDaemonConfig } from "./daemon-config.js";
import { layerLoomRpcHandlers } from "./rpc-handlers.js";

const codeKernelEntry = new URL("../../code-kernel/src/main.ts", import.meta.url).pathname;

const layerJobSupervisor = layerJobRecovery.pipe(
  Layer.tap((services) => {
    const reconciler = Context.get(services, JobReconciler);
    return reconciler.reconcile.pipe(
      Effect.tap((results) => Effect.logInfo("Job restart reconciliation completed.", results)),
    );
  }),
);

export const runLoomDaemon = <E, R>(
  config: DaemonConfig,
  capabilities: Layer.Layer<WorkflowCapabilityExecutor | WorkflowArtifactStore, E, R>,
) =>
  Effect.gen(function* () {
    yield* prepareDaemonSocket(config.socketPath);
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fs.makeDirectory(path.dirname(config.databasePath), { recursive: true });
    return yield* Effect.gen(function* () {
      const daemonStartedAtMillis = yield* Clock.currentTimeMillis;
      const cluster = SingleRunner.layer({
        runnerStorage: "sql",
        shardingConfig: { entityTerminationTimeout: "1 second" },
      });
      const agents = layerAgentActor.pipe(
        Layer.provide([
          layerSqliteCellJournal,
          layerCodeKernelFactory({
            entryPath: codeKernelEntry,
            diagnosticsDirectory: `${config.workspaceRoot}/.loom/diagnostics/code-kernels`,
          }),
        ]),
      );
      const childAgents = layerSqliteWorkflowChildAgentStore;
      const workflows = layerLoomWorkflowRuntime.pipe(
        Layer.provide([capabilities, layerActorStateHub]),
      );
      const application = Layer.mergeAll(agents, childAgents, workflows).pipe(
        Layer.provide(cluster),
        Layer.provideMerge(layerJobSupervisor),
      );
      const handlers = layerLoomRpcHandlers.pipe(
        Layer.provide(application),
        Layer.provide(
          layerConnectionHandshake({
            workspaceRoot: config.workspaceRoot,
            daemonStartedAtMillis,
          }),
        ),
      );
      const server = layerBunLoomServer({ socketPath: config.socketPath }).pipe(
        Layer.provide(handlers),
        Layer.tap(() => Effect.logInfo("Loom daemon is ready")),
      );
      return yield* Layer.launch(server);
    }).pipe(Effect.provide(layerLoomSqlite({ filename: config.databasePath })));
  });

export const program = Effect.gen(function* () {
  const config = yield* loadDaemonConfig;
  const capabilities = layerWorkflowCapabilities({
    workspaceRoot: config.workspaceRoot,
  }).pipe(
    Layer.provide([
      layerSqliteWorkflowChildAgentStore,
      layerSqliteWorkflowJobStore,
      layerSqliteJobProcessStore,
      layerBunProcessInspector,
    ]),
  );
  return yield* runLoomDaemon(config, capabilities);
});
