import {
  layerBunLoomServer,
  layerBunJobRuntime,
  layerBunProcessController,
  layerBunProcessInspector,
  layerCodeKernelFactory,
  layerLoomSqlite,
  layerLoomWorkflowRuntime,
  layerSqliteJobStore,
  layerSqlitePluginStateStore,
  layerSqliteSessionClosureStore,
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
  layerSqliteCellLedger,
  layerSqliteCodeKernelProcessStore,
  prepareDaemonSocket,
  defaultBunWorkflowAgentPolicy,
} from "@cvr/loom-platform-bun";
import {
  JobRuntime,
  CellLedger,
  CodeKernelProcessStore,
  ProcessController,
  ProcessInspector,
  SessionClosureStore,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  WorkflowRunRecovery,
  layerSessionLifecycle,
  layerActorStateHub,
  layerAgentActorWith,
  layerConnectionHandshake,
  reconcileCodeKernelProcesses,
} from "@cvr/loom-runtime";
import { Clock, Context, type Duration, Effect, FileSystem, Layer, Path } from "effect";
import { SingleRunner } from "effect/unstable/cluster";
import { type DaemonConfig, loadDaemonConfig } from "./daemon-config.js";
import { layerLoomRpcHandlers } from "./rpc-handlers.js";
import { layerSessionRecovery, SessionRecovery } from "./session-recovery.js";

export type { DaemonConfig } from "./daemon-config.js";

const codeKernelEntry = new URL("../../code-kernel/src/main.ts", import.meta.url).pathname;

export interface DaemonPolicy {
  readonly codeKernelIdleLease: Duration.Input;
  readonly entityIdleLease: Duration.Input;
  readonly sessionClosureLease: Duration.Input;
}

export const defaultDaemonPolicy = {
  codeKernelIdleLease: "5 minutes",
  entityIdleLease: "1 minute",
  sessionClosureLease: "5 minutes",
} satisfies DaemonPolicy;

export interface DaemonRecoveryPhases<E1, E2, E3, E4, E5, E6, E7> {
  readonly sessionClosures: Effect.Effect<void, E1>;
  readonly codeKernels: Effect.Effect<void, E2>;
  readonly cells: Effect.Effect<void, E3>;
  readonly jobs: Effect.Effect<void, E4>;
  readonly closedSessions: Effect.Effect<void, E5>;
  readonly workflowRetirement: Effect.Effect<void, E6>;
  readonly workflows: Effect.Effect<void, E7>;
}

export const runRecoveryPhases = <E1, E2, E3, E4, E5, E6, E7>(
  phases: DaemonRecoveryPhases<E1, E2, E3, E4, E5, E6, E7>,
) =>
  phases.sessionClosures.pipe(
    Effect.andThen(phases.codeKernels),
    Effect.andThen(phases.cells),
    Effect.andThen(phases.jobs),
    Effect.andThen(phases.closedSessions),
    Effect.andThen(phases.workflowRetirement),
    Effect.andThen(phases.workflows),
  );

export const recoverDaemon = Effect.gen(function* () {
  const sessions = yield* SessionClosureStore;
  const store = yield* CodeKernelProcessStore;
  const inspector = yield* ProcessInspector;
  const controller = yield* ProcessController;
  const cells = yield* CellLedger;
  const jobs = yield* JobRuntime;
  const workflows = yield* WorkflowRunRecovery;
  const sessionsToRecover = yield* SessionRecovery;
  return yield* runRecoveryPhases({
    sessionClosures: sessions.prune.pipe(
      Effect.tap((count) => Effect.logInfo("Session closure pruning completed.", { count })),
      Effect.asVoid,
    ),
    codeKernels: reconcileCodeKernelProcesses({ store, inspector, controller }),
    cells: cells.reconcile,
    jobs: jobs.reconcile.pipe(
      Effect.tap((results) =>
        Effect.logInfo("Job reconciliation completed.", { count: results.length }),
      ),
      Effect.asVoid,
    ),
    closedSessions: sessionsToRecover.recover,
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
  | SessionClosureStore
  | SessionRecovery
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

const makeClusterLayer = (policy: DaemonPolicy) =>
  SingleRunner.layer({
    runnerStorage: "sql",
    shardingConfig: {
      entityMaxIdleTime: policy.entityIdleLease,
      entityMessagePollInterval: "100 millis",
      entityTerminationTimeout: "1 second",
    },
  });

const makeSessionLayer = (policy: DaemonPolicy) =>
  layerSessionLifecycle({ closureLease: policy.sessionClosureLease }).pipe(
    Layer.provideMerge(layerSqliteSessionClosureStore),
  );

const launchDaemon = <E, R, E2>(
  config: DaemonConfig,
  capabilities: Layer.Layer<WorkflowCapabilityExecutor | WorkflowArtifactStore, E, R>,
  recover: DaemonRecovery<E2>,
  policy: DaemonPolicy,
) =>
  Effect.gen(function* () {
    const daemonStartedAtMillis = yield* Clock.currentTimeMillis;
    const cluster = makeClusterLayer(policy);
    const actors = layerActorStateHub;
    const jobs = makeJobLayer(config, actors);
    const childAgents = layerSqliteWorkflowChildAgentStore;
    const sessions = makeSessionLayer(policy);
    const orchestration = capabilities.pipe(Layer.provideMerge(sessions));
    const workflows = layerLoomWorkflowRuntime.pipe(
      Layer.provide([orchestration, actors, childAgents]),
    );
    const baseApplication = Layer.mergeAll(
      actors,
      makeAgentLayer(config, policy),
      childAgents,
      layerSqlitePluginStateStore,
      orchestration,
      workflows,
    ).pipe(Layer.provideMerge(jobs), Layer.provide(cluster));
    const application = Layer.merge(
      baseApplication,
      layerSessionRecovery.pipe(Layer.provide(baseApplication)),
    ).pipe(Layer.tap((services) => recover(services)));
    const handlers = layerLoomRpcHandlers.pipe(
      Layer.provide(application),
      Layer.provide(
        layerConnectionHandshake({
          workspaceRoot: config.workspaceRoot,
          daemonStartedAtMillis,
          codeKernelIdleLease: policy.codeKernelIdleLease,
        }),
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
    ...defaultBunWorkflowAgentPolicy,
  }).pipe(Layer.provide(layerSqliteWorkflowChildAgentStore));
  return yield* runLoomDaemon(config, capabilities);
});
