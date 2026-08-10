import {
  layerBunLoomServer,
  layerBunJobRuntime,
  layerBunProcessController,
  layerCodeKernelFactory,
  layerBunProcessInspector,
  layerLoomSqlite,
  layerLoomWorkflowRuntime,
  layerSqliteJobStore,
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
  layerSqliteCellJournal,
  prepareDaemonSocket,
} from "@cvr/loom-platform-bun";
import {
  JobRuntime,
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

const makeAgentLayer = (config: DaemonConfig) =>
  layerAgentActor.pipe(
    Layer.provide([
      layerSqliteCellJournal,
      layerCodeKernelFactory({
        entryPath: codeKernelEntry,
        diagnosticsDirectory: `${config.workspaceRoot}/.loom/diagnostics/code-kernels`,
      }),
    ]),
  );

const makeJobLayer = (config: DaemonConfig, actors: typeof layerActorStateHub) =>
  layerBunJobRuntime({
    workspaceRoot: config.workspaceRoot,
    terminationGrace: "5 seconds",
  }).pipe(
    Layer.provide([
      actors,
      layerBunProcessController,
      layerBunProcessInspector,
      layerSqliteJobStore,
    ]),
    Layer.tap((services) => {
      const runtime = Context.get(services, JobRuntime);
      return runtime.reconcile.pipe(
        Effect.tap((results) => Effect.logInfo("Job reconciliation completed.", results)),
      );
    }),
  );

const launchDaemon = <E, R>(
  config: DaemonConfig,
  capabilities: Layer.Layer<WorkflowCapabilityExecutor | WorkflowArtifactStore, E, R>,
) =>
  Effect.gen(function* () {
    const daemonStartedAtMillis = yield* Clock.currentTimeMillis;
    const cluster = SingleRunner.layer({
      runnerStorage: "sql",
      shardingConfig: { entityTerminationTimeout: "1 second" },
    });
    const actors = layerActorStateHub;
    const jobs = makeJobLayer(config, actors);
    const workflows = layerLoomWorkflowRuntime.pipe(Layer.provide([capabilities, actors]));
    const application = Layer.mergeAll(
      actors,
      makeAgentLayer(config),
      layerSqliteWorkflowChildAgentStore,
      workflows,
    ).pipe(Layer.provideMerge(jobs), Layer.provide(cluster));
    const handlers = layerLoomRpcHandlers.pipe(
      Layer.provide(application),
      Layer.provide(
        layerConnectionHandshake({ workspaceRoot: config.workspaceRoot, daemonStartedAtMillis }),
      ),
    );
    const server = layerBunLoomServer({ socketPath: config.socketPath }).pipe(
      Layer.provide(handlers),
      Layer.tap(() => Effect.logInfo("Loom daemon is ready")),
    );
    return yield* Layer.launch(server);
  });

export const runLoomDaemon = <E, R>(
  config: DaemonConfig,
  capabilities: Layer.Layer<WorkflowCapabilityExecutor | WorkflowArtifactStore, E, R>,
) =>
  Effect.gen(function* () {
    yield* prepareDaemonSocket(config.socketPath);
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fs.makeDirectory(path.dirname(config.databasePath), { recursive: true });
    return yield* launchDaemon(config, capabilities).pipe(
      Effect.provide(layerLoomSqlite({ filename: config.databasePath })),
    );
  });

export const program = Effect.gen(function* () {
  const config = yield* loadDaemonConfig;
  const capabilities = layerWorkflowCapabilities({
    workspaceRoot: config.workspaceRoot,
  }).pipe(Layer.provide(layerSqliteWorkflowChildAgentStore));
  return yield* runLoomDaemon(config, capabilities);
});
