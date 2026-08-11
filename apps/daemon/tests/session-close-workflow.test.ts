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
  defaultBunWorkflowAgentPolicy,
  layerLoomSqlite,
  layerSqliteJobStore,
  layerSqliteSessionClosureStore,
  layerSqliteWorkflowChildAgentStore,
  layerWorkflowCapabilities,
} from "@cvr/loom-platform-bun";
import { JobStore, SessionClosureStore } from "@cvr/loom-runtime";
import { workflowInterpreterVersion, WorkflowRunState } from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Array as Arr, Effect, Fiber, FileSystem, Layer, Option, Schedule } from "effect";
import { runLoomDaemon } from "../src/program.js";
import { withClient } from "./workflow-test-support.js";

const sessionId = SessionId.make("session-close-workflow");

const requestFor = (startedPath: string) =>
  WorkflowRunRequest.make({
    sessionId,
    key: WorkflowKey.make("active"),
    definition: WorkflowDefinition.make({
      name: WorkflowName.make("session-close-workflow"),
      version: WorkflowVersion.make("1"),
      interpreterVersion: workflowInterpreterVersion,
      source: `return await step.run({
        stepId: "job", capability: "job", input: { command: input.command },
      })`,
      capabilities: [WorkflowCapability.make("job")],
      signals: [],
    }),
    input: { command: `: > '${startedPath}'; while true; do sleep 0.05; done` },
    budget: WorkflowBudget.make({
      maxSteps: 1,
      maxAgentRuns: 1,
      maxParallelism: 1,
      maxInlineStepResultBytes: 1_024,
      maxTokens: Option.none(),
      maxDurationMillis: Option.none(),
    }),
  });

const databaseLayer = (databasePath: string) => layerLoomSqlite({ filename: databasePath });

const writeSessionClosure = (databasePath: string) =>
  SessionClosureStore.pipe(
    Effect.flatMap((closures) => closures.close(sessionId, "5 minutes")),
    Effect.provide(layerSqliteSessionClosureStore.pipe(Layer.provide(databaseLayer(databasePath)))),
    Effect.scoped,
  );

const waitForActiveJob = (databasePath: string) =>
  JobStore.pipe(
    Effect.flatMap((jobs) => jobs.listRecoverable),
    Effect.repeat({
      while: (jobs) => jobs.length === 0,
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.flatMap((jobs) =>
      Option.match(Arr.head(jobs), {
        onNone: () => Effect.die("The Job did not start."),
        onSome: Effect.succeed,
      }),
    ),
    Effect.provide(layerSqliteJobStore.pipe(Layer.provide(databaseLayer(databasePath)))),
  );

const waitForFile = (path: string) =>
  FileSystem.FileSystem.pipe(
    Effect.flatMap((fs) => fs.exists(path)),
    Effect.repeat({
      until: (exists) => exists,
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.timeout("5 seconds"),
  );

it.scopedLive.layer(BunServices.layer)(
  "suspends an Activity after Session closure before safe interruption",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-session-suspend-" });
      const workspaceRoot = WorkspaceRoot.make(directory);
      const socketPath = `${directory}/daemon.sock`;
      const databasePath = `${directory}/loom.sqlite`;
      const startedPath = `${directory}/started`;
      const capabilities = layerWorkflowCapabilities({
        workspaceRoot,
        ...defaultBunWorkflowAgentPolicy,
      }).pipe(Layer.provide(layerSqliteWorkflowChildAgentStore));
      const daemon = yield* runLoomDaemon(
        { workspaceRoot, socketPath, databasePath },
        capabilities,
      ).pipe(Effect.forkScoped);

      yield* withClient(workspaceRoot, socketPath, (client) =>
        Effect.gen(function* () {
          const handle = yield* client.startWorkflow(requestFor(startedPath));
          yield* waitForFile(startedPath);
          const job = yield* waitForActiveJob(databasePath);
          yield* writeSessionClosure(databasePath);
          yield* client.cancelJob({ sessionId, jobId: job.jobId });
          const address = { sessionId, workflowRunId: handle.workflowRunId };
          const suspended = yield* client.inspectWorkflow(address).pipe(
            Effect.repeat({
              while: WorkflowRunState.guards.Pending,
              schedule: Schedule.spaced("10 millis"),
            }),
            Effect.timeout("5 seconds"),
          );
          expect(suspended).toHaveProperty("_tag", "Suspended");
          yield* client.interruptWorkflow(address);
          expect(
            yield* client.inspectWorkflow(address).pipe(
              Effect.repeat({
                while: (state) => !WorkflowRunState.guards.Interrupted(state),
                schedule: Schedule.spaced("10 millis"),
              }),
              Effect.timeout("5 seconds"),
            ),
          ).toHaveProperty("_tag", "Interrupted");
        }),
      );
      yield* Fiber.interrupt(daemon);
    }),
  30_000,
);
