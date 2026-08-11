import { BunServices } from "@effect/platform-bun";
import type { LoomClientShape } from "@cvr/loom-client";
import {
  JobId,
  PluginId,
  PluginStateAddress,
  PluginStateScope,
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  type WorkflowRunAddress,
  WorkflowRunId,
  WorkflowRunRequest,
  WorkflowSignalName,
  WorkflowVersion,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  layerLoomSqlite,
  layerSqliteSessionClosureStore,
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
  defaultBunWorkflowAgentPolicy,
} from "@cvr/loom-platform-bun";
import { SessionClosureStore } from "@cvr/loom-runtime";
import {
  PluginStateReadResult,
  PluginStateVersion,
  workflowInterpreterVersion,
  WorkflowRunState,
} from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Layer, Option, Schedule } from "effect";
import { runLoomDaemon } from "../src/program.js";
import { withClient } from "./workflow-test-support.js";

const sessionId = SessionId.make("session-close");
const attachedJobId = JobId.make("session-close-attached");
const detachedJobId = JobId.make("session-close-detached");
const pluginId = PluginId.make("session-close-test");
const lateWorkflowRunId = WorkflowRunId.make("session-close-late");
const lateSignalName = WorkflowSignalName.make("continue");
const sessionPluginState = PluginStateAddress.make({
  pluginId,
  scope: PluginStateScope.cases.Session.make({ sessionId }),
  key: PluginStateAddress.fields.key.make("state"),
});
const workspacePluginState = PluginStateAddress.make({
  pluginId,
  scope: PluginStateScope.cases.Workspace.make({}),
  key: PluginStateAddress.fields.key.make("state"),
});
const capabilitiesFor = (workspaceRoot: WorkspaceRoot) =>
  layerWorkflowCapabilities({
    workspaceRoot,
    ...defaultBunWorkflowAgentPolicy,
  }).pipe(Layer.provide(layerSqliteWorkflowChildAgentStore));

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

const seedPluginState = (client: LoomClientShape) =>
  Effect.all(
    [
      client.writePluginState({
        address: sessionPluginState,
        expected: PluginStateVersion.cases.Missing.make({}),
        value: { owner: "session" },
      }),
      client.writePluginState({
        address: workspacePluginState,
        expected: PluginStateVersion.cases.Missing.make({}),
        value: { owner: "workspace" },
      }),
    ],
    { discard: true },
  );

const startSessionWork = Effect.fn("SessionCloseTest.startWork")(function* (
  client: LoomClientShape,
  request: WorkflowRunRequest,
  workflowStarted: string,
) {
  yield* seedPluginState(client);
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
  expect(terminal).toHaveProperty("_tag", "Interrupted");
  expect((yield* client.inspectJob({ sessionId, jobId: attachedJobId })).status).toBe("Cancelled");
  expect((yield* client.inspectJob({ sessionId, jobId: detachedJobId })).status).toBe("Running");
  expect(yield* client.readPluginState({ address: sessionPluginState })).toEqual(
    PluginStateReadResult.cases.Missing.make({}),
  );
  expect(yield* client.readPluginState({ address: workspacePluginState })).toHaveProperty(
    "_tag",
    "Present",
  );
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
  const pluginStateError = yield* client
    .writePluginState({
      address: sessionPluginState,
      expected: PluginStateVersion.cases.Missing.make({}),
      value: { owner: "late" },
    })
    .pipe(Effect.flip);
  const signalError = yield* client
    .signalWorkflow({
      address: { sessionId, workflowRunId: lateWorkflowRunId, name: lateSignalName },
      value: {},
    })
    .pipe(Effect.flip);
  expect(jobError).toHaveProperty("_tag", "SessionClosingError");
  expect(workflowError).toHaveProperty("_tag", "SessionClosingError");
  expect(pluginStateError).toHaveProperty("_tag", "SessionClosingError");
  expect(signalError).toHaveProperty("_tag", "SessionClosingError");
});

const writeSessionClosure = (databasePath: string) => {
  const database = layerLoomSqlite({ filename: databasePath });
  return SessionClosureStore.pipe(
    Effect.flatMap((closures) => closures.close(sessionId, "5 minutes")),
    Effect.provide(layerSqliteSessionClosureStore.pipe(Layer.provide(database))),
    Effect.scoped,
  );
};

const seedCrashWork = (client: LoomClientShape) =>
  seedPluginState(client).pipe(
    Effect.andThen(
      client.startJob({
        sessionId,
        jobId: attachedJobId,
        command: "sleep 30",
        attached: true,
        foregroundLeaseMillis: 20,
      }),
    ),
  );

const verifyRecoveredClose = Effect.fn("SessionCloseTest.verifyRecoveredClose")(function* (
  client: LoomClientShape,
) {
  expect((yield* client.inspectJob({ sessionId, jobId: attachedJobId })).status).toBe("Cancelled");
  expect(yield* client.readPluginState({ address: sessionPluginState })).toEqual(
    PluginStateReadResult.cases.Missing.make({}),
  );
  yield* verifyLateWork(client);
});

it.scopedLive.layer(BunServices.layer)(
  "interrupts Session work and preserves a detached Job",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-session-close-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const socketPath = `${directory}/daemon.sock`;
      const databasePath = `${directory}/loom.sqlite`;
      const workflowStarted = `${directory}/workflow-started`;
      const capabilities = capabilitiesFor(workspaceRoot);
      const daemon = yield* runLoomDaemon(
        { workspaceRoot, socketPath, databasePath },
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

      const restarted = yield* runLoomDaemon(
        { workspaceRoot, socketPath, databasePath },
        capabilities,
      ).pipe(Effect.forkScoped);
      yield* withClient(workspaceRoot, socketPath, verifyLateWork);
      yield* Fiber.interrupt(restarted);
    }),
  30_000,
);

it.scopedLive.layer(BunServices.layer)(
  "replays Session cleanup after a crash",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-session-recovery-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const socketPath = `${directory}/daemon.sock`;
      const databasePath = `${directory}/loom.sqlite`;
      const capabilities = capabilitiesFor(workspaceRoot);
      const daemon = yield* runLoomDaemon(
        { workspaceRoot, socketPath, databasePath },
        capabilities,
      ).pipe(Effect.forkScoped);

      yield* withClient(workspaceRoot, socketPath, seedCrashWork);
      yield* writeSessionClosure(databasePath);
      yield* Fiber.interrupt(daemon);

      const restarted = yield* runLoomDaemon(
        { workspaceRoot, socketPath, databasePath },
        capabilities,
      ).pipe(Effect.forkScoped);
      yield* withClient(workspaceRoot, socketPath, verifyRecoveredClose);
      yield* Fiber.interrupt(restarted);
    }),
  30_000,
);
