import { SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { WorkflowRunState, type WorkflowCompensationDecision } from "@cvr/loom-protocol";
import { WorkflowStepError, WorkflowStepExecution } from "@cvr/loom-runtime";
import { expect } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Ref, Schedule } from "effect";
import { runLoomDaemon } from "../src/program.js";
import { activityRestartRequest, compensationRestartRequest } from "./workflow-restart-fixtures.js";
import { scopedLive, testCapabilities, withClient } from "./workflow-test-support.js";

const makeHarness = Effect.gen(function* () {
  const completedStepCompensationAttempts = yield* Ref.make(0);
  const blockedStepCompensationAttempts = yield* Ref.make(0);
  const capabilities = testCapabilities({
    supports: () => true,
    execute: (call) => {
      if (call.stepId === "failed") {
        return Effect.fail(
          new WorkflowStepError({
            stepId: call.stepId,
            capability: call.capability,
            message: "failed",
          }),
        );
      }
      return Effect.succeed(
        WorkflowStepExecution.make({ value: call.input, tokenCount: 0, agentRuns: 0 }),
      );
    },
    compensate: (call) => {
      if (call.stepId === "blocked-compensation") {
        return Ref.update(blockedStepCompensationAttempts, (count) => count + 1);
      }
      return Effect.gen(function* () {
        const attempt = yield* Ref.updateAndGet(
          completedStepCompensationAttempts,
          (count) => count + 1,
        );
        if (attempt > 1) return;
        return yield* new WorkflowStepError({
          stepId: call.stepId,
          capability: call.capability,
          message: "compensation failed",
        });
      });
    },
  });
  return { capabilities, blockedStepCompensationAttempts, completedStepCompensationAttempts };
});

const proveDecision = (
  decision: WorkflowCompensationDecision,
  expectedFailedCompensations: number,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-compensation-decision-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const socketPath = `${directory}/daemon.sock`;
    const { capabilities, blockedStepCompensationAttempts, completedStepCompensationAttempts } =
      yield* makeHarness;
    const daemon = yield* runLoomDaemon(
      { workspaceRoot, socketPath, databasePath: `${directory}/loom.sqlite` },
      capabilities,
    ).pipe(Effect.forkScoped);

    yield* withClient(workspaceRoot, socketPath, (client) =>
      Effect.gen(function* () {
        const handle = yield* client.startWorkflow(compensationRestartRequest);
        const address = { sessionId: compensationRestartRequest.sessionId, ...handle };
        yield* client.inspectWorkflow(address).pipe(
          Effect.repeat({
            while: (state) => !WorkflowRunState.guards.Suspended(state),
            schedule: Schedule.spaced("10 millis"),
          }),
        );

        const foreignError = yield* client
          .decideWorkflowCompensation({
            address: { ...address, sessionId: SessionId.make("foreign-session") },
            decision,
          })
          .pipe(Effect.flip);
        expect(foreignError).toHaveProperty("_tag", "WorkflowRunNotFoundError");

        yield* client.decideWorkflowCompensation({
          address,
          decision,
        });
        const error = yield* client.executeWorkflow(compensationRestartRequest).pipe(Effect.flip);
        expect(error).toHaveProperty("_tag", "WorkflowStepError");
        const terminal = yield* client.inspectWorkflow(address);
        expect(WorkflowRunState.guards.Failure(terminal)).toBe(true);
      }),
    );

    expect(yield* Ref.get(completedStepCompensationAttempts)).toBe(expectedFailedCompensations);
    expect(yield* Ref.get(blockedStepCompensationAttempts)).toBe(1);
    yield* Fiber.interrupt(daemon);
  });

scopedLive(
  "retries a failed compensation through the daemon RPC",
  () => proveDecision("Retry", 2),
  30_000,
);

scopedLive(
  "stops a failed compensation through the daemon RPC",
  () => proveDecision("Stop", 1),
  30_000,
);

scopedLive(
  "rejects a compensation decision when no compensation is pending",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-no-compensation-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const socketPath = `${directory}/daemon.sock`;
      const { capabilities } = yield* makeHarness;
      const daemon = yield* runLoomDaemon(
        { workspaceRoot, socketPath, databasePath: `${directory}/loom.sqlite` },
        capabilities,
      ).pipe(Effect.forkScoped);

      yield* withClient(workspaceRoot, socketPath, (client) =>
        Effect.gen(function* () {
          const handle = yield* client.startWorkflow(activityRestartRequest);
          yield* client.executeWorkflow(activityRestartRequest);
          const error = yield* client
            .decideWorkflowCompensation({
              address: { sessionId: activityRestartRequest.sessionId, ...handle },
              decision: "Retry",
            })
            .pipe(Effect.flip);

          expect(error).toHaveProperty("_tag", "WorkflowCompensationNotPendingError");
        }),
      );
      yield* Fiber.interrupt(daemon);
    }),
  30_000,
);
