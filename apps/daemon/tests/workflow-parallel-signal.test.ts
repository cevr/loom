import {
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  WorkflowRunRequest,
  WorkflowSignalName,
  WorkflowVersion,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import { workflowInterpreterVersion, WorkflowRunState } from "@cvr/loom-protocol";
import {
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
} from "@cvr/loom-platform-bun";
import { WorkflowJobHandle } from "@cvr/loom-runtime";
import { expect } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Layer, Option, Schedule, Schema } from "effect";
import { runLoomDaemon } from "../src/program.js";
import { scopedLive, waitForSuspension, withClient } from "./workflow-test-support.js";

const parallelRequest = (
  firstCommand: string,
  secondCommand: string,
  key: string,
  durationMillis = 5_000,
) =>
  WorkflowRunRequest.make({
    sessionId: SessionId.make(`parallel-${key}`),
    key: WorkflowKey.make(key),
    definition: WorkflowDefinition.make({
      name: WorkflowName.make("parallel-job-signal"),
      version: WorkflowVersion.make("1"),
      interpreterVersion: workflowInterpreterVersion,
      source: `
        const [first, second] = await Promise.all([
          step.run({
            stepId: "first", capability: "job", input: { command: input.firstCommand },
          }),
          step.run({
            stepId: "second", capability: "job", input: { command: input.secondCommand },
          }),
        ])
        const approval = await signal.wait("approval")
        return { first, second, approval }
      `,
      capabilities: [WorkflowCapability.make("job")],
      signals: [WorkflowSignalName.make("approval")],
    }),
    input: { firstCommand, secondCommand },
    budget: WorkflowBudget.make({
      maxSteps: 2,
      maxAgentRuns: 1,
      maxParallelism: 2,
      maxInlineStepResultBytes: 1_024,
      maxTokens: Option.none(),
      maxDurationMillis: Option.some(durationMillis),
    }),
  });

const resultSchema = Schema.Struct({
  first: WorkflowJobHandle,
  second: WorkflowJobHandle,
  approval: Schema.String,
});

const startDaemon = (directory: string) => {
  const workspaceRoot = WorkspaceRoot.make(directory);
  const config = {
    workspaceRoot,
    socketPath: `${directory}/daemon.sock`,
    databasePath: `${directory}/loom.sqlite`,
  };
  const capabilities = layerWorkflowCapabilities({ workspaceRoot }).pipe(
    Layer.provide(layerSqliteWorkflowChildAgentStore),
  );
  return { config, daemon: runLoomDaemon(config, capabilities).pipe(Effect.forkScoped) };
};

const waitForFile = (path: string) =>
  FileSystem.FileSystem.pipe(
    Effect.flatMap((fs) => fs.exists(path)),
    Effect.repeat({ while: (exists) => !exists, schedule: Schedule.spaced("10 millis") }),
    Effect.timeout("5 seconds"),
  );

scopedLive(
  "resumes after parallel Job Steps reach a Signal",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-parallel-signal-" });
      const request = parallelRequest(
        ": > first-complete",
        ": > second-complete",
        "parallel-success",
      );
      const { config, daemon: launch } = startDaemon(directory);
      const firstDaemon = yield* launch;
      const handle = yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
        client.startWorkflow(request),
      );
      const address = { sessionId: request.sessionId, ...handle };

      expect(
        yield* waitForSuspension(config.workspaceRoot, config.socketPath, address),
      ).toHaveProperty("_tag", "Suspended");
      yield* Fiber.interrupt(firstDaemon);

      const secondDaemon = yield* startDaemon(directory).daemon;
      yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
        client.signalWorkflow({
          address: { ...address, name: WorkflowSignalName.make("approval") },
          value: "approved",
        }),
      );
      const result = yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
        client.executeWorkflow(request),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(resultSchema)));

      expect(result.approval).toBe("approved");
      expect(yield* fs.exists(`${directory}/first-complete`)).toBe(true);
      expect(yield* fs.exists(`${directory}/second-complete`)).toBe(true);
      yield* Fiber.interrupt(secondDaemon);
    }),
  30_000,
);

scopedLive(
  "expires the duration budget while parallel Job results wait at a Signal",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-parallel-duration-" });
      const request = parallelRequest(
        ": > first-complete",
        ": > second-complete",
        "parallel-duration",
        2_000,
      );
      const { config, daemon: launch } = startDaemon(directory);
      const firstDaemon = yield* launch;
      const handle = yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
        client.startWorkflow(request),
      );
      const address = { sessionId: request.sessionId, ...handle };

      expect(
        yield* waitForSuspension(config.workspaceRoot, config.socketPath, address),
      ).toHaveProperty("_tag", "Suspended");
      yield* Fiber.interrupt(firstDaemon);

      const secondDaemon = yield* startDaemon(directory).daemon;
      const terminal = yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
        client.inspectWorkflow(address),
      ).pipe(
        Effect.repeat({
          while: (state) =>
            WorkflowRunState.guards.Pending(state) || WorkflowRunState.guards.Suspended(state),
          schedule: Schedule.spaced("10 millis"),
        }),
        Effect.timeout("5 seconds"),
      );

      expect(terminal).toHaveProperty("_tag", "Failure");
      expect(terminal).toHaveProperty("error._tag", "WorkflowBudgetExceededError");
      expect(terminal).toHaveProperty("error.budget", "Duration");
      yield* Fiber.interrupt(secondDaemon);
    }),
  30_000,
);

scopedLive(
  "keeps a parallel Job failure ahead of the duration budget",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-parallel-failure-" });
      const request = parallelRequest(": > first-complete", "exit 7", "parallel-failure");
      const { config, daemon: launch } = startDaemon(directory);
      const daemon = yield* launch;

      const error = yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
        client.executeWorkflow(request),
      ).pipe(Effect.flip);

      expect(error).toHaveProperty("_tag", "WorkflowStepError");
      expect(error).toHaveProperty("stepId", "second");
      expect(error).toHaveProperty("message", "The Job exited with code 7.");
      expect(yield* fs.exists(`${directory}/first-complete`)).toBe(true);
      yield* Fiber.interrupt(daemon);
    }),
  30_000,
);

scopedLive(
  "reconciles an in-flight parallel Job batch through a daemon restart",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-parallel-restart-" });
      const releasePath = `${directory}/release-second`;
      const firstLaunches = `${directory}/first-launches`;
      const secondLaunches = `${directory}/second-launches`;
      const secondStarted = `${directory}/second-started`;
      const request = parallelRequest(
        `printf x >> '${firstLaunches}'`,
        `printf x >> '${secondLaunches}'; : > '${secondStarted}'; while [ ! -f '${releasePath}' ]; do sleep 0.05; done`,
        "parallel-in-flight-restart",
        10_000,
      );
      yield* Effect.addFinalizer(() =>
        fs.writeFileString(releasePath, "release").pipe(Effect.orDie),
      );
      const { config, daemon: launch } = startDaemon(directory);
      const firstDaemon = yield* launch;
      const handle = yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
        client.startWorkflow(request),
      );
      const address = { sessionId: request.sessionId, ...handle };

      expect(yield* waitForFile(secondStarted)).toBe(true);
      yield* Fiber.interrupt(firstDaemon);

      const secondDaemon = yield* startDaemon(directory).daemon;
      yield* withClient(config.workspaceRoot, config.socketPath, (client) => client.handshake);
      yield* fs.writeFileString(releasePath, "release");
      expect(
        yield* waitForSuspension(config.workspaceRoot, config.socketPath, address),
      ).toHaveProperty("_tag", "Suspended");
      yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
        client.signalWorkflow({
          address: { ...address, name: WorkflowSignalName.make("approval") },
          value: "approved",
        }),
      );
      const result = yield* withClient(config.workspaceRoot, config.socketPath, (client) =>
        client.executeWorkflow(request),
      ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(resultSchema)));

      expect(result.approval).toBe("approved");
      expect(yield* fs.readFileString(firstLaunches)).toBe("x");
      expect(yield* fs.readFileString(secondLaunches)).toBe("x");
      expect(result.first.jobId).not.toBe(result.second.jobId);
      yield* Fiber.interrupt(secondDaemon);
    }),
  30_000,
);
