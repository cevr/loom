import {
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  WorkflowRunRequest,
  WorkflowVersion,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
} from "@cvr/loom-platform-bun";
import { expect } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Layer, Option } from "effect";
import { runLoomDaemon } from "../src/program.js";
import { scopedLive, withClient } from "./workflow-test-support.js";

const request = WorkflowRunRequest.make({
  sessionId: SessionId.make("job-failure-session"),
  key: WorkflowKey.make("job-failure"),
  definition: WorkflowDefinition.make({
    name: WorkflowName.make("job-failure"),
    version: WorkflowVersion.make("1"),
    interpreterVersion: 1,
    source: `
      await step.run({
        stepId: "prepare",
        capability: "job",
        input: { command: ": > prepared" },
      })
      await step.run({
        stepId: "verify",
        capability: "job",
        input: { command: ": > failed; exit 7" },
      })
      await step.run({
        stepId: "publish",
        capability: "job",
        input: { command: ": > published" },
      })
      return "published"
    `,
    capabilities: [WorkflowCapability.make("job")],
    signals: [],
  }),
  input: {},
  budget: WorkflowBudget.make({
    maxSteps: 3,
    maxAgentRuns: 1,
    maxParallelism: 1,
    maxInlineStepResultBytes: 1_024,
    maxTokens: Option.none(),
    maxDurationMillis: Option.none(),
  }),
});

scopedLive(
  "keeps dependent Jobs stopped after a failed Job and daemon restart",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-workflow-job-failure-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const socketPath = `${directory}/daemon.sock`;
      const config = {
        workspaceRoot,
        socketPath,
        databasePath: `${directory}/loom.sqlite`,
      };
      const capabilities = layerWorkflowCapabilities({ workspaceRoot }).pipe(
        Layer.provide(layerSqliteWorkflowChildAgentStore),
      );

      const firstDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
      const firstError = yield* withClient(workspaceRoot, socketPath, (client) =>
        client.executeWorkflow(request),
      ).pipe(Effect.flip);

      expect(firstError).toHaveProperty("_tag", "WorkflowStepError");
      expect(firstError).toHaveProperty("stepId", "verify");
      expect(yield* fs.exists(`${directory}/prepared`)).toBe(true);
      expect(yield* fs.exists(`${directory}/failed`)).toBe(true);
      expect(yield* fs.exists(`${directory}/published`)).toBe(false);
      yield* Fiber.interrupt(firstDaemon);

      const secondDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
      const replayError = yield* withClient(workspaceRoot, socketPath, (client) =>
        client.executeWorkflow(request),
      ).pipe(Effect.flip);

      expect(replayError).toEqual(firstError);
      expect(yield* fs.exists(`${directory}/published`)).toBe(false);
      yield* Fiber.interrupt(secondDaemon);
    }),
  30_000,
);
