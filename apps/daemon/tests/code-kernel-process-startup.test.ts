import { BunServices } from "@effect/platform-bun";
import {
  AgentId,
  CellId,
  CodeKernelProcessRecord,
  SessionId,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteCodeKernelProcessStore,
  makeBunProcessController,
} from "@cvr/loom-platform-bun";
import { EvaluateCellRequest } from "@cvr/loom-protocol";
import { CodeKernelProcessStore, ProcessInspector, ProcessObservation } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Array as Arr, Effect, Fiber, FileSystem, Layer, Option, Schedule } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import {
  type DaemonConfig,
  type DaemonPolicy,
  defaultDaemonPolicy,
  runLoomDaemon,
} from "../src/program.js";
import { testCapabilities, withClient } from "./workflow-test-support.js";

const scopedLive = it.scopedLive.layer(BunServices.layer);

const persistence = (filename: string) =>
  layerSqliteCodeKernelProcessStore.pipe(Layer.provideMerge(layerLoomSqlite({ filename })));

const readProcesses = (filename: string) =>
  CodeKernelProcessStore.use((store) => store.list).pipe(Effect.provide(persistence(filename)));

const daemonCapabilities = testCapabilities({
  supports: () => false,
  execute: () => Effect.die("This test does not run a Workflow capability."),
  compensate: () => Effect.void,
});

const startDaemon = (config: DaemonConfig, policy: DaemonPolicy = defaultDaemonPolicy) =>
  runLoomDaemon(config, daemonCapabilities, policy).pipe(Effect.forkScoped);

const seedOrphan = Effect.fn("Test.seedCodeKernelOrphan")(function* (databasePath: string) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const child = yield* spawner.spawn(
    ChildProcess.make("sleep", ["30"], {
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
    }),
  );
  const processIdentity = yield* ProcessInspector.use((inspector) =>
    inspector.inspect(child.pid).pipe(
      Effect.flatMap(
        ProcessObservation.$match({
          Missing: () => Effect.die(`Process ${child.pid} is missing.`),
          Found: ({ identity }) => Effect.succeed(identity),
        }),
      ),
    ),
  ).pipe(Effect.provide(layerBunProcessInspector));
  yield* CodeKernelProcessStore.use((store) =>
    store.register(
      CodeKernelProcessRecord.make({
        sessionId: SessionId.make("session-startup"),
        agentId: AgentId.make("agent-startup"),
        ...processIdentity,
      }),
    ),
  ).pipe(Effect.provide(persistence(databasePath)));
  return processIdentity;
});

scopedLive("reconciles a Code Kernel process before the daemon accepts a client", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-startup-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const databasePath = `${directory}/loom.sqlite`;
    const socketPath = `${directory}/daemon.sock`;
    const identity = yield* seedOrphan(databasePath);

    const daemon = yield* startDaemon({ workspaceRoot, socketPath, databasePath });
    yield* withClient(workspaceRoot, socketPath, (client) => client.handshake);

    expect(yield* makeBunProcessController.isGroupAlive(identity)).toBe(false);
    expect(yield* readProcesses(databasePath)).toEqual([]);
    yield* Fiber.interrupt(daemon);
  }),
);

scopedLive(
  "releases the exact Code Kernel process when the idle lease ends",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-idle-expiry-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const databasePath = `${directory}/loom.sqlite`;
      const socketPath = `${directory}/daemon.sock`;
      const owner = {
        sessionId: SessionId.make("session-idle-expiry"),
        agentId: AgentId.make("agent-idle-expiry"),
      };
      const daemon = yield* startDaemon(
        { workspaceRoot, socketPath, databasePath },
        { codeKernelIdleLease: "100 millis", entityIdleLease: "100 millis" },
      );

      yield* withClient(workspaceRoot, socketPath, (client) =>
        client.evaluateCell(
          EvaluateCellRequest.make({
            ...owner,
            cellId: CellId.make("cell-before-expiry"),
            source: "const releasedAfterIdle = 42",
          }),
        ),
      );
      const process = yield* Option.match(Arr.head(yield* readProcesses(databasePath)), {
        onNone: () => Effect.die("The Code Kernel process was not registered."),
        onSome: Effect.succeed,
      });
      expect(
        yield* readProcesses(databasePath).pipe(
          Effect.repeat({
            while: (processes) => processes.length > 0,
            schedule: Schedule.spaced("50 millis"),
          }),
          Effect.timeout("8 seconds"),
        ),
      ).toEqual([]);
      expect(yield* makeBunProcessController.isGroupAlive(process)).toBe(false);

      yield* Fiber.interrupt(daemon);
    }),
  15_000,
);

scopedLive(
  "keeps Code Kernel bindings through the interactive idle lease",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-idle-lease-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const databasePath = `${directory}/loom.sqlite`;
      const socketPath = `${directory}/daemon.sock`;
      const owner = {
        sessionId: SessionId.make("session-idle-lease"),
        agentId: AgentId.make("agent-idle-lease"),
      };
      const daemon = yield* startDaemon(
        { workspaceRoot, socketPath, databasePath },
        { codeKernelIdleLease: "10 seconds", entityIdleLease: "100 millis" },
      );

      yield* withClient(workspaceRoot, socketPath, (client) =>
        client.evaluateCell(
          EvaluateCellRequest.make({
            ...owner,
            cellId: CellId.make("cell-before-idle"),
            source: "const retainedAcrossIdle = 42",
          }),
        ),
      );
      yield* Effect.sleep("5500 millis");
      const retained = yield* withClient(workspaceRoot, socketPath, (client) =>
        client.evaluateCell(
          EvaluateCellRequest.make({
            ...owner,
            cellId: CellId.make("cell-after-idle"),
            source: "retainedAcrossIdle",
          }),
        ),
      );

      expect(retained.display).toBe("42");
      yield* Fiber.interrupt(daemon);
    }),
  15_000,
);
