import { BunServices } from "@effect/platform-bun";
import { AgentId, CodeKernelProcessRecord, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import {
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteCodeKernelProcessStore,
  makeBunProcessController,
} from "@cvr/loom-platform-bun";
import { CodeKernelProcessStore, ProcessInspector, ProcessObservation } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Layer } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { runLoomDaemon } from "../src/program.js";
import { testCapabilities, withClient } from "./workflow-test-support.js";

const scopedLive = it.scopedLive.layer(BunServices.layer);

const persistence = (filename: string) =>
  layerSqliteCodeKernelProcessStore.pipe(Layer.provideMerge(layerLoomSqlite({ filename })));

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
    const processStore = persistence(databasePath);

    const daemon = yield* runLoomDaemon(
      { workspaceRoot, socketPath, databasePath },
      testCapabilities({
        supports: () => false,
        execute: () => Effect.die("This test does not run a Workflow capability."),
        compensate: () => Effect.void,
      }),
    ).pipe(Effect.forkScoped);
    yield* withClient(workspaceRoot, socketPath, (client) => client.handshake);

    expect(yield* makeBunProcessController.isGroupAlive(identity)).toBe(false);
    expect(
      yield* CodeKernelProcessStore.use((store) => store.list).pipe(Effect.provide(processStore)),
    ).toEqual([]);
    yield* Fiber.interrupt(daemon);
  }),
);
