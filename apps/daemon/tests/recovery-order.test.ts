import { WorkspaceRoot } from "@cvr/loom-domain";
import {
  CellLedger,
  CodeKernelProcessStore,
  JobRuntime,
  ProcessController,
  ProcessInspector,
  SessionClosureStore,
  WorkflowRunRecovery,
} from "@cvr/loom-runtime";
import { expect } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Layer, Queue, Ref } from "effect";
import { recoverDaemon, runLoomDaemonWithRecovery, runRecoveryPhases } from "../src/program.js";
import { SessionRecovery } from "../src/session-recovery.js";
import { scopedLive, testCapabilities, withClient } from "./workflow-test-support.js";

type RecoveryPhase =
  | "session-closures"
  | "code-kernels"
  | "cells"
  | "jobs"
  | "closed-sessions"
  | "workflow-retirement"
  | "workflows";

const phaseNames: ReadonlyArray<RecoveryPhase> = [
  "session-closures",
  "code-kernels",
  "cells",
  "jobs",
  "closed-sessions",
  "workflow-retirement",
  "workflows",
];

const unused = Effect.die("Unused recovery service operation.");

const sessionRecoveryServices = (phase: (name: RecoveryPhase) => Effect.Effect<void>) =>
  Layer.merge(
    Layer.succeed(SessionRecovery, SessionRecovery.of({ recover: phase("closed-sessions") })),
    Layer.succeed(
      SessionClosureStore,
      SessionClosureStore.of({
        close: () => unused,
        contains: () => unused,
        list: Effect.succeed([]),
        prune: phase("session-closures").pipe(Effect.as(0)),
      }),
    ),
  );

const processRecoveryServices = (phase: (name: RecoveryPhase) => Effect.Effect<void>) =>
  Layer.mergeAll(
    Layer.succeed(
      CodeKernelProcessStore,
      CodeKernelProcessStore.of({
        register: () => unused,
        remove: () => unused,
        list: phase("code-kernels").pipe(Effect.as([])),
      }),
    ),
    Layer.succeed(ProcessInspector, ProcessInspector.of({ inspect: () => unused })),
    Layer.succeed(
      ProcessController,
      ProcessController.of({ isGroupAlive: () => unused, signalGroup: () => unused }),
    ),
  );

const cellRecoveryService = (phase: (name: RecoveryPhase) => Effect.Effect<void>) =>
  Layer.succeed(
    CellLedger,
    CellLedger.of({
      claim: () => unused,
      evaluating: () => unused,
      complete: () => unused,
      reconcile: phase("cells"),
    }),
  );

const jobRecoveryService = (phase: (name: RecoveryPhase) => Effect.Effect<void>) =>
  Layer.succeed(
    JobRuntime,
    JobRuntime.of({
      start: () => unused,
      inspect: () => unused,
      await: () => unused,
      awaitTerminal: () => unused,
      readOutput: () => unused,
      cancel: () => unused,
      detach: () => unused,
      closeSession: () => unused,
      reconcile: phase("jobs").pipe(Effect.as([])),
    }),
  );

const recoveryServices = (phase: (name: RecoveryPhase) => Effect.Effect<void>) =>
  Layer.mergeAll(
    sessionRecoveryServices(phase),
    processRecoveryServices(phase),
    cellRecoveryService(phase),
    jobRecoveryService(phase),
    Layer.succeed(
      WorkflowRunRecovery,
      WorkflowRunRecovery.of({
        retire: phase("workflow-retirement"),
        recover: phase("workflows"),
      }),
    ),
  );

scopedLive("maps each production recovery service to the documented phase", () =>
  Effect.gen(function* () {
    const calls = yield* Ref.make<ReadonlyArray<RecoveryPhase>>([]);
    const phase = (name: RecoveryPhase) => Ref.update(calls, (current) => [...current, name]);

    yield* recoverDaemon.pipe(Effect.provide(recoveryServices(phase)));
    expect(yield* Ref.get(calls)).toEqual(phaseNames);
  }),
);

scopedLive("runs every recovery phase before the daemon accepts a client", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-recovery-order-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const socketPath = `${directory}/daemon.sock`;
    const events = yield* Queue.unbounded<string>();
    const releases = yield* Queue.unbounded<true>();
    const phase = (name: RecoveryPhase) =>
      Queue.offer(events, `${name}:started`).pipe(
        Effect.andThen(Queue.take(releases)),
        Effect.andThen(Queue.offer(events, `${name}:completed`)),
        Effect.asVoid,
      );
    const recovery = runRecoveryPhases({
      sessionClosures: phase("session-closures"),
      codeKernels: phase("code-kernels"),
      cells: phase("cells"),
      jobs: phase("jobs"),
      closedSessions: phase("closed-sessions"),
      workflowRetirement: phase("workflow-retirement"),
      workflows: phase("workflows"),
    });
    const daemon = yield* runLoomDaemonWithRecovery(
      { workspaceRoot, socketPath, databasePath: `${directory}/loom.sqlite` },
      testCapabilities({
        supports: () => false,
        execute: () => Effect.die("This test does not run a Workflow capability."),
        compensate: () => Effect.void,
      }),
      () => recovery,
    ).pipe(Effect.forkScoped);

    for (const name of phaseNames) {
      expect(yield* Queue.take(events)).toBe(`${name}:started`);
      expect(yield* fs.exists(socketPath)).toBe(false);
      yield* Queue.offer(releases, true);
      expect(yield* Queue.take(events)).toBe(`${name}:completed`);
    }

    yield* withClient(workspaceRoot, socketPath, (client) => client.handshake);
    yield* Fiber.interrupt(daemon);
  }),
);
