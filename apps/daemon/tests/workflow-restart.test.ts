import { WorkflowSignalName, WorkspaceRoot } from "@cvr/loom-domain";
import { WorkflowRunState } from "@cvr/loom-protocol";
import { WorkflowStepExecution, WorkflowStepError } from "@cvr/loom-runtime";
import { expect } from "effect-bun-test";
import { Deferred, Effect, Fiber, FileSystem, Ref, Schedule } from "effect";
import { runLoomDaemon } from "../src/program.js";
import {
  activityRestartRequest,
  compensationRestartRequest,
  signalRestartRequest,
} from "./workflow-restart-fixtures.js";
import {
  scopedLive,
  testCapabilities,
  waitForSuspension,
  withClient,
} from "./workflow-test-support.js";
const blockUntilRestart = (
  counter: Ref.Ref<number>,
  started: Deferred.Deferred<true>,
  release: Deferred.Deferred<true>,
) =>
  Effect.uninterruptibleMask((restore) =>
    Ref.update(counter, (count) => count + 1).pipe(
      Effect.andThen(Deferred.succeed(started, true)),
      Effect.andThen(restore(Deferred.await(release))),
      Effect.asVoid,
    ),
  );

const makeRestartHarness = Effect.gen(function* () {
  const completed = yield* Ref.make(0);
  const blocked = yield* Ref.make(0);
  const activityStarted = yield* Deferred.make<true>();
  const releaseActivity = yield* Deferred.make<true>();
  const capabilities = testCapabilities({
    supports: () => true,
    execute: (call) => {
      if (call.stepId === "completed") {
        return Ref.update(completed, (count) => count + 1).pipe(
          Effect.as(WorkflowStepExecution.make({ value: call.input, tokenCount: 0, agentRuns: 0 })),
        );
      }
      return blockUntilRestart(blocked, activityStarted, releaseActivity).pipe(
        Effect.as(WorkflowStepExecution.make({ value: call.input, tokenCount: 0, agentRuns: 0 })),
      );
    },
    compensate: () => Effect.void,
  });
  return { activityStarted, blocked, capabilities, completed, releaseActivity };
});

const makeSignalRestartHarness = Effect.gen(function* () {
  const completed = yield* Ref.make(0);
  const resumed = yield* Ref.make(0);
  const capabilities = testCapabilities({
    supports: () => true,
    execute: (call) => {
      const result = WorkflowStepExecution.make({
        value: call.input,
        tokenCount: 0,
        agentRuns: 0,
      });
      if (call.stepId === "completed") {
        return Ref.update(completed, (count) => count + 1).pipe(Effect.as(result));
      }
      return Ref.update(resumed, (count) => count + 1).pipe(Effect.as(result));
    },
    compensate: () => Effect.void,
  });
  return { capabilities, completed, resumed };
});

interface CompensationRestartState {
  readonly blocked: Ref.Ref<number>;
  readonly blockedCompensations: Ref.Ref<number>;
  readonly completed: Ref.Ref<number>;
  readonly completedCompensations: Ref.Ref<number>;
  readonly compensationOrder: Ref.Ref<ReadonlyArray<string>>;
  readonly compensationStarted: Deferred.Deferred<true>;
  readonly failed: Ref.Ref<number>;
  readonly releaseCompensation: Deferred.Deferred<true>;
}

const compensationCapabilities = (state: CompensationRestartState) =>
  testCapabilities({
    supports: () => true,
    execute: (call) => {
      if (call.stepId === "failed") {
        return Ref.update(state.failed, (count) => count + 1).pipe(
          Effect.andThen(
            Effect.fail(
              new WorkflowStepError({
                stepId: call.stepId,
                capability: call.capability,
                message: "failed",
              }),
            ),
          ),
        );
      }
      if (call.stepId === "blocked-compensation") {
        return Ref.update(state.blocked, (count) => count + 1).pipe(
          Effect.as(WorkflowStepExecution.make({ value: call.input, tokenCount: 0, agentRuns: 0 })),
        );
      }
      return Ref.update(state.completed, (count) => count + 1).pipe(
        Effect.as(WorkflowStepExecution.make({ value: call.input, tokenCount: 0, agentRuns: 0 })),
      );
    },
    compensate: (call) => {
      if (call.stepId === "completed-compensation") {
        return Ref.update(state.completedCompensations, (count) => count + 1).pipe(
          Effect.andThen(Ref.update(state.compensationOrder, (order) => [...order, "completed"])),
        );
      }
      return Ref.update(state.compensationOrder, (order) => [...order, "blocked"]).pipe(
        Effect.andThen(
          blockUntilRestart(
            state.blockedCompensations,
            state.compensationStarted,
            state.releaseCompensation,
          ),
        ),
      );
    },
  });

const makeCompensationRestartHarness = Effect.gen(function* () {
  const state: CompensationRestartState = {
    blocked: yield* Ref.make(0),
    blockedCompensations: yield* Ref.make(0),
    completed: yield* Ref.make(0),
    completedCompensations: yield* Ref.make(0),
    compensationOrder: yield* Ref.make<ReadonlyArray<string>>([]),
    compensationStarted: yield* Deferred.make<true>(),
    failed: yield* Ref.make(0),
    releaseCompensation: yield* Deferred.make<true>(),
  };
  return { ...state, capabilities: compensationCapabilities(state) };
});

scopedLive(
  "recovers an Activity through a full daemon restart",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-daemon-workflow-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const socketPath = `${directory}/daemon.sock`;
      const config = {
        workspaceRoot,
        socketPath,
        databasePath: `${directory}/loom.sqlite`,
      };
      const { activityStarted, blocked, capabilities, completed, releaseActivity } =
        yield* makeRestartHarness;

      const firstDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
      yield* withClient(workspaceRoot, socketPath, (client) => client.handshake);
      yield* withClient(workspaceRoot, socketPath, (client) =>
        client.startWorkflow(activityRestartRequest),
      );
      yield* Deferred.await(activityStarted);
      yield* Fiber.interrupt(firstDaemon);

      const secondDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
      yield* withClient(workspaceRoot, socketPath, (client) => client.handshake);
      yield* Deferred.succeed(releaseActivity, true);
      const result = yield* withClient(workspaceRoot, socketPath, (client) =>
        client.executeWorkflow(activityRestartRequest),
      );

      expect(result).toEqual({ completed: "completed", resumed: "resumed" });
      expect(yield* Ref.get(completed)).toBe(1);
      expect(yield* Ref.get(blocked)).toBe(2);
      yield* Fiber.interrupt(secondDaemon);
    }),
  30_000,
);

scopedLive(
  "wakes a signal-waiting Workflow in a fresh VM pass after daemon restart",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-daemon-resume-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const socketPath = `${directory}/daemon.sock`;
      const config = {
        workspaceRoot,
        socketPath,
        databasePath: `${directory}/loom.sqlite`,
      };
      const { capabilities, completed, resumed } = yield* makeSignalRestartHarness;

      const firstDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
      const handle = yield* withClient(workspaceRoot, socketPath, (client) =>
        client.startWorkflow(signalRestartRequest),
      );
      const address = { sessionId: signalRestartRequest.sessionId, ...handle };
      yield* Ref.get(completed).pipe(
        Effect.repeat({
          while: (count) => count !== 1,
          schedule: Schedule.spaced("10 millis"),
        }),
      );
      expect(yield* waitForSuspension(workspaceRoot, socketPath, address)).toEqual(
        WorkflowRunState.cases.Suspended.make({}),
      );
      yield* Fiber.interrupt(firstDaemon);

      const secondDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
      const waiting = yield* withClient(workspaceRoot, socketPath, (client) =>
        client.inspectWorkflow(address),
      );
      expect(waiting).toEqual(WorkflowRunState.cases.Suspended.make({}));

      yield* withClient(workspaceRoot, socketPath, (client) =>
        client.signalWorkflow({
          address: { ...address, name: WorkflowSignalName.make("continue") },
          value: "continued",
        }),
      );
      const result = yield* withClient(workspaceRoot, socketPath, (client) =>
        client.executeWorkflow(signalRestartRequest),
      );

      expect(result).toEqual({ completed: "completed", signal: "continued", resumed: "continued" });
      expect(yield* Ref.get(completed)).toBe(1);
      expect(yield* Ref.get(resumed)).toBe(1);
      yield* Fiber.interrupt(secondDaemon);
    }),
  30_000,
);

scopedLive(
  "recovers a compensation through a full daemon restart",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-daemon-compensation-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const socketPath = `${directory}/daemon.sock`;
      const config = {
        workspaceRoot,
        socketPath,
        databasePath: `${directory}/loom.sqlite`,
      };
      const {
        blocked,
        blockedCompensations,
        capabilities,
        compensationStarted,
        compensationOrder,
        completed,
        completedCompensations,
        failed,
        releaseCompensation,
      } = yield* makeCompensationRestartHarness;

      const firstDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
      yield* withClient(workspaceRoot, socketPath, (client) =>
        client.startWorkflow(compensationRestartRequest),
      );
      yield* Deferred.await(compensationStarted);
      yield* Fiber.interrupt(firstDaemon);

      const secondDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
      yield* withClient(workspaceRoot, socketPath, (client) => client.handshake);
      yield* Deferred.succeed(releaseCompensation, true);
      const error = yield* withClient(workspaceRoot, socketPath, (client) =>
        client.executeWorkflow(compensationRestartRequest),
      ).pipe(Effect.flip);

      expect(error).toHaveProperty("_tag", "WorkflowStepError");
      expect(error).toHaveProperty("stepId", "failed");
      expect(yield* Ref.get(blocked)).toBe(1);
      expect(yield* Ref.get(completed)).toBe(1);
      expect(yield* Ref.get(failed)).toBe(1);
      expect(yield* Ref.get(completedCompensations)).toBe(1);
      expect(yield* Ref.get(blockedCompensations)).toBe(2);
      expect(yield* Ref.get(compensationOrder)).toEqual(["completed", "blocked", "blocked"]);
      yield* Fiber.interrupt(secondDaemon);
    }),
  30_000,
);
