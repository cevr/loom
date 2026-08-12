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
import { Effect, Layer, Ref } from "effect";
import { recoverDaemon } from "../src/program.js";
import { SessionRecovery } from "../src/session-recovery.js";
import { scopedLive } from "./workflow-test-support.js";

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
