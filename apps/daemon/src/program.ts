import {
  layerBunLoomServer,
  layerBunJobRuntime,
  layerBunProcessController,
  layerBunProcessInspector,
  layerCodeKernelFactory,
  layerLoomSqlite,
  layerLoomWorkflowRuntime,
  layerSqliteJobStore,
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
  layerSqliteCellLedger,
  layerSqliteCodeKernelProcessStore,
  prepareDaemonSocket,
} from "@cvr/loom-platform-bun";
import {
  JobRuntime,
  CellLedger,
  CodeKernelProcessStore,
  ProcessController,
  ProcessInspector,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  WorkflowRunRecovery,
  layerActorStateHub,
  layerAgentActorWith,
  layerConnectionHandshake,
  reconcileCodeKernelProcesses,
} from "@cvr/loom-runtime";
import { Clock, Context, type Duration, Effect, FileSystem, Layer, Path } from "effect";
import { SingleRunner } from "effect/unstable/cluster";
import { type DaemonConfig, loadDaemonConfig } from "./daemon-config.js";
import { layerLoomRpcHandlers } from "./rpc-handlers.js";

export type { DaemonConfig } from "./daemon-config.js";

const codeKernelEntry = new URL("../../code-kernel/src/main.ts", import.meta.url).pathname;

export interface DaemonPolicy {
  readonly codeKernelIdleLease: Duration.Input;
  readonly entityIdleLease: Duration.Input;
}

export const defaultDaemonPolicy = {
  codeKernelIdleLease: "5 minutes",
  entityIdleLease: "1 minute",
} satisfies DaemonPolicy;

export interface DaemonRecoveryPhases<E1, E2, E3, E4, E5> {
  readonly codeKernels: Effect.Effect<void, E1>;
  readonly cells: Effect.Effect<void, E2>;
  readonly jobs: Effect.Effect<void, E3>;
  readonly workflowRetirement: Effect.Effect<void, E4>;
  readonly workflows: Effect.Effect<void, E5>;
}

export const runRecoveryPhases = <E1, E2, E3, E4, E5>(
  phases: DaemonRecoveryPhases<E1, E2, E3, E4, E5>,
) =>
  phases.codeKernels.pipe(
    Effect.andThen(phases.cells),
    Effect.andThen(phases.jobs),
    Effect.andThen(phases.workflowRetirement),
    Effect.andThen(phases.workflows),
  );

export const recoverDaemon = Effect.gen(function* () {
  const store = yield* CodeKernelProcessStore;
  const inspector = yield* ProcessInspector;
  const controller = yield* ProcessController;
  const cells = yield* CellLedger;
  const jobs = yield* JobRuntime;
  const workflows = yield* WorkflowRunRecovery;
  return yield* runRecoveryPhases({
    codeKernels: reconcileCodeKernelProcesses({ store, inspector, controller }),
    cells: cells.reconcile,
    jobs: jobs.reconcile.pipe(
      Effect.tap((results) =>
        Effect.logInfo("Job reconciliation completed.", { count: results.length }),
      ),
      Effect.asVoid,
    ),
    workflowRetirement: workflows.retire,
    workflows: workflows.recover,
  });
});

type RecoveryServices =
  | CellLedger
  | CodeKernelProcessStore
  | JobRuntime
  | ProcessController
  | ProcessInspector
  | WorkflowRunRecovery;

type DaemonRecovery<E> = (services: Context.Context<RecoveryServices>) => Effect.Effect<void, E>;

const recoverApplication: DaemonRecovery<Effect.Error<typeof recoverDaemon>> = (services) =>
  recoverDaemon.pipe(Effect.provide(services));

const makeAgentLayer = (config: DaemonConfig, policy: DaemonPolicy) => {
  const processServices = Layer.mergeAll(
    layerSqliteCodeKernelProcessStore,
    layerBunProcessInspector,
    layerBunProcessController,
  );
  const kernelFactory = layerCodeKernelFactory({
    entryPath: codeKernelEntry,
    diagnosticsDirectory: `${config.workspaceRoot}/.loom/diagnostics/code-kernels`,
  }).pipe(Layer.provideMerge(processServices));
  const dependencies = Layer.merge(kernelFactory, layerSqliteCellLedger);
  return layerAgentActorWith({ idleLease: policy.codeKernelIdleLease }).pipe(
    Layer.provideMerge(dependencies),
  );
};

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
  );

const launchDaemon = <E, R, E2>(
  config: DaemonConfig,
  capabilities: Layer.Layer<WorkflowCapabilityExecutor | WorkflowArtifactStore, E, R>,
  recover: DaemonRecovery<E2>,
  policy: DaemonPolicy,
) =>
  Effect.gen(function* () {
    const daemonStartedAtMillis = yield* Clock.currentTimeMillis;
    const cluster = SingleRunner.layer({
      runnerStorage: "sql",
      shardingConfig: {
        entityMaxIdleTime: policy.entityIdleLease,
        entityMessagePollInterval: "100 millis",
        entityTerminationTimeout: "1 second",
      },
    });
    const actors = layerActorStateHub;
    const jobs = makeJobLayer(config, actors);
    const workflows = layerLoomWorkflowRuntime.pipe(Layer.provide([capabilities, actors]));
    const application = Layer.mergeAll(
      actors,
      makeAgentLayer(config, policy),
      layerSqliteWorkflowChildAgentStore,
      workflows,
    ).pipe(
      Layer.provideMerge(jobs),
      Layer.provide(cluster),
      Layer.tap((services) => recover(services)),
    );
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

export const runLoomDaemonWithRecovery = <E, R, E2>(
  config: DaemonConfig,
  capabilities: Layer.Layer<WorkflowCapabilityExecutor | WorkflowArtifactStore, E, R>,
  recover: DaemonRecovery<E2>,
  policy: DaemonPolicy = defaultDaemonPolicy,
) =>
  Effect.gen(function* () {
    yield* prepareDaemonSocket(config.socketPath);
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    yield* fs.makeDirectory(path.dirname(config.databasePath), { recursive: true });
    return yield* launchDaemon(config, capabilities, recover, policy).pipe(
      Effect.provide(layerLoomSqlite({ filename: config.databasePath })),
    );
  });

export const runLoomDaemon = <E, R>(
  config: DaemonConfig,
  capabilities: Layer.Layer<WorkflowCapabilityExecutor | WorkflowArtifactStore, E, R>,
  policy: DaemonPolicy = defaultDaemonPolicy,
) => runLoomDaemonWithRecovery(config, capabilities, recoverApplication, policy);

export const program = Effect.gen(function* () {
  const config = yield* loadDaemonConfig;
  const capabilities = layerWorkflowCapabilities({
    workspaceRoot: config.workspaceRoot,
  }).pipe(Layer.provide(layerSqliteWorkflowChildAgentStore));
  return yield* runLoomDaemon(config, capabilities);
});
