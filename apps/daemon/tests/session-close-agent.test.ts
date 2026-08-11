import { BunServices } from "@effect/platform-bun";
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
  layerLoomSqlite,
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
} from "@cvr/loom-platform-bun";
import { WorkflowChildAgentStore } from "@cvr/loom-runtime";
import { workflowInterpreterVersion, WorkflowRunState } from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Layer, Option, Schedule } from "effect";
import { runLoomDaemon } from "../src/program.js";
import { withClient } from "./workflow-test-support.js";

const sessionId = SessionId.make("session-close-agent");
const workflowAgentFixture = new URL(
  "../../../packages/platform-bun/tests/fixtures/workflow-agent.ts",
  import.meta.url,
).pathname;

const activeAgentWorkflow = (releasePath: string) =>
  WorkflowRunRequest.make({
    sessionId,
    key: WorkflowKey.make("active-agent"),
    definition: WorkflowDefinition.make({
      name: WorkflowName.make("session-close-agent"),
      version: WorkflowVersion.make("1"),
      interpreterVersion: workflowInterpreterVersion,
      source: `return await step.run({
        stepId: "agent", capability: "agent", input: { prompt: input.prompt },
      })`,
      capabilities: [WorkflowCapability.make("agent")],
      signals: [],
    }),
    input: { prompt: `wait-for:${releasePath}` },
    budget: WorkflowBudget.make({
      maxSteps: 1,
      maxAgentRuns: 1,
      maxParallelism: 1,
      maxInlineStepResultBytes: 1_024,
      maxTokens: Option.none(),
      maxDurationMillis: Option.none(),
    }),
  });

const capabilitiesFor = (workspaceRoot: WorkspaceRoot) =>
  layerWorkflowCapabilities({
    workspaceRoot,
    executable: "bun",
    arguments: ["run", workflowAgentFixture],
    maximumOutputBytes: 64 * 1_024,
  }).pipe(Layer.provide(layerSqliteWorkflowChildAgentStore));

const readActiveChildAgents = (databasePath: string) => {
  const database = layerLoomSqlite({ filename: databasePath });
  return WorkflowChildAgentStore.pipe(
    Effect.flatMap((agents) => agents.listActiveBySession(sessionId)),
    Effect.provide(layerSqliteWorkflowChildAgentStore.pipe(Layer.provide(database))),
  );
};

it.scopedLive.layer(BunServices.layer)(
  "interrupts a Workflow child Agent when its Session closes",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-agent-session-close-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const socketPath = `${directory}/daemon.sock`;
      const databasePath = `${directory}/loom.sqlite`;
      const releasePath = `${directory}/release-agent`;
      yield* Effect.addFinalizer(() =>
        fs.writeFileString(releasePath, "release").pipe(Effect.orDie),
      );
      const daemon = yield* runLoomDaemon(
        { workspaceRoot, socketPath, databasePath },
        capabilitiesFor(workspaceRoot),
      ).pipe(Effect.forkScoped);

      yield* withClient(workspaceRoot, socketPath, (client) =>
        Effect.gen(function* () {
          const handle = yield* client.startWorkflow(activeAgentWorkflow(releasePath));
          const [agent] = yield* readActiveChildAgents(databasePath).pipe(
            Effect.repeat({
              while: (agents) => agents.length === 0,
              schedule: Schedule.spaced("10 millis"),
            }),
            Effect.timeout("5 seconds"),
          );
          if (!agent) return yield* Effect.die("The child Agent did not start.");
          yield* client.closeSession(sessionId);
          expect(
            yield* client.inspectWorkflow({ sessionId, workflowRunId: handle.workflowRunId }).pipe(
              Effect.repeat({
                while: (state) =>
                  WorkflowRunState.guards.Pending(state) ||
                  WorkflowRunState.guards.Suspended(state),
                schedule: Schedule.spaced("10 millis"),
              }),
              Effect.timeout("5 seconds"),
            ),
          ).toHaveProperty("_tag", "Interrupted");
          expect((yield* client.inspectJob({ sessionId, jobId: agent.jobId })).status).toBe(
            "Cancelled",
          );
          expect(yield* readActiveChildAgents(databasePath)).toEqual([]);
        }),
      );
      yield* Fiber.interrupt(daemon);
    }),
  30_000,
);
