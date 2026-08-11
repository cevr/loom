import { BunServices } from "@effect/platform-bun";
import type { LoomClientShape } from "@cvr/loom-client";
import {
  JobId,
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  type WorkflowRunAddress,
  WorkflowRunRequest,
  WorkflowVersion,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
  defaultBunWorkflowAgentPolicy,
} from "@cvr/loom-platform-bun";
import { workflowInterpreterVersion, WorkflowRunState } from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Layer, Option, Schedule } from "effect";
import { runLoomDaemon } from "../src/program.js";
import { withClient } from "./workflow-test-support.js";

const sessionId = SessionId.make("session-close");
const attachedJobId = JobId.make("session-close-attached");
const detachedJobId = JobId.make("session-close-detached");

const activeWorkflow = (key: string, command: string) =>
  WorkflowRunRequest.make({
    sessionId,
    key: WorkflowKey.make(key),
    definition: WorkflowDefinition.make({
      name: WorkflowName.make("session-close"),
      version: WorkflowVersion.make("1"),
      interpreterVersion: workflowInterpreterVersion,
      source: `return await step.run({
        stepId: "job", capability: "job", input: { command: input.command },
      })`,
      capabilities: [WorkflowCapability.make("job")],
      signals: [],
    }),
    input: { command },
    budget: WorkflowBudget.make({
      maxSteps: 1,
      maxAgentRuns: 1,
      maxParallelism: 1,
      maxInlineStepResultBytes: 1_024,
      maxTokens: Option.none(),
      maxDurationMillis: Option.none(),
    }),
  });

const waitForFile = (path: string) =>
  FileSystem.FileSystem.pipe(
    Effect.flatMap((fs) => fs.exists(path)),
    Effect.repeat({
      until: (exists) => exists,
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.timeout("5 seconds"),
  );

const startSessionWork = Effect.fn("SessionCloseTest.startWork")(function* (
  client: LoomClientShape,
  request: WorkflowRunRequest,
  workflowStarted: string,
) {
  const handle = yield* client.startWorkflow(request);
  yield* waitForFile(workflowStarted);
  yield* client.startJob({
    sessionId,
    jobId: attachedJobId,
    command: "sleep 30",
    attached: true,
    foregroundLeaseMillis: 20,
  });
  yield* client.startJob({
    sessionId,
    jobId: detachedJobId,
    command: "sleep 30",
    attached: false,
    foregroundLeaseMillis: 20,
  });
  return { sessionId, ...handle };
});

const verifyClosedWork = Effect.fn("SessionCloseTest.verifyClosedWork")(function* (
  client: LoomClientShape,
  address: WorkflowRunAddress,
) {
  yield* client.closeSession(sessionId);
  yield* client.closeSession(sessionId);
  const terminal = yield* client.inspectWorkflow(address).pipe(
    Effect.repeat({
      while: (state) =>
        WorkflowRunState.guards.Pending(state) || WorkflowRunState.guards.Suspended(state),
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.timeout("5 seconds"),
  );
  expect(terminal).toHaveProperty("_tag", "Failure");
  expect(terminal).toHaveProperty("error._tag", "SessionClosingError");
  expect((yield* client.inspectJob({ sessionId, jobId: attachedJobId })).status).toBe("Cancelled");
  expect((yield* client.inspectJob({ sessionId, jobId: detachedJobId })).status).toBe("Running");
});

const verifyLateWork = Effect.fn("SessionCloseTest.verifyLateWork")(function* (
  client: LoomClientShape,
) {
  const jobError = yield* client
    .startJob({
      sessionId,
      jobId: JobId.make("session-close-late"),
      command: "true",
      attached: true,
      foregroundLeaseMillis: 20,
    })
    .pipe(Effect.flip);
  const workflowError = yield* client
    .startWorkflow(activeWorkflow("late", "true"))
    .pipe(Effect.flip);
  expect(jobError).toHaveProperty("_tag", "SessionClosingError");
  expect(workflowError).toHaveProperty("_tag", "SessionClosingError");
});

it.scopedLive.layer(BunServices.layer)(
  "interrupts Session work and preserves a detached Job",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-session-close-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const socketPath = `${directory}/daemon.sock`;
      const workflowStarted = `${directory}/workflow-started`;
      const capabilities = layerWorkflowCapabilities({
        workspaceRoot,
        ...defaultBunWorkflowAgentPolicy,
      }).pipe(Layer.provide(layerSqliteWorkflowChildAgentStore));
      const daemon = yield* runLoomDaemon(
        { workspaceRoot, socketPath, databasePath: `${directory}/loom.sqlite` },
        capabilities,
      ).pipe(Effect.forkScoped);
      const request = activeWorkflow(
        "active",
        `: > '${workflowStarted}'; while true; do sleep 0.05; done`,
      );

      yield* withClient(workspaceRoot, socketPath, (client) =>
        Effect.gen(function* () {
          const address = yield* startSessionWork(client, request, workflowStarted);
          yield* verifyClosedWork(client, address);
          yield* verifyLateWork(client);
          yield* client.cancelJob({ sessionId, jobId: detachedJobId });
        }),
      );
      yield* Fiber.interrupt(daemon);
    }),
  30_000,
);
