import { BunServices } from "@effect/platform-bun";
import { LoomClient } from "@cvr/loom-client";
import {
  ArtifactId,
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
import { layerBunLoomClient } from "@cvr/loom-platform-bun";
import {
  WorkflowArtifactReference,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  WorkflowStepExecution,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Deferred, Effect, Fiber, FileSystem, Layer, Option, Ref } from "effect";
import { runLoomDaemon } from "../src/program.js";

const request = WorkflowRunRequest.make({
  sessionId: SessionId.make("daemon-restart-session"),
  key: WorkflowKey.make("activity-restart"),
  definition: WorkflowDefinition.make({
    name: WorkflowName.make("daemon-restart"),
    version: WorkflowVersion.make("1"),
    interpreterVersion: 1,
    source: `
      const completed = await step.run({
        stepId: "completed",
        capability: "test",
        input: "completed",
      })
      const resumed = await step.run({
        stepId: "blocked",
        capability: "test",
        input: "resumed",
      })
      return { completed, resumed }
    `,
    capabilities: [WorkflowCapability.make("test")],
    signals: [],
  }),
  input: {},
  budget: WorkflowBudget.make({
    maxSteps: 2,
    maxAgentRuns: 1,
    maxParallelism: 1,
    maxInlineStepResultBytes: 1_024,
    maxTokens: Option.none(),
    maxDurationMillis: Option.none(),
  }),
});

const withClient = <A, E, R>(
  workspaceRoot: WorkspaceRoot,
  socketPath: string,
  effect: Effect.Effect<A, E, R | LoomClient>,
) =>
  effect.pipe(
    Effect.provide(
      layerBunLoomClient({ workspaceRoot, socketPath, connectionTimeout: "10 seconds" }),
    ),
  );

const scopedLive = it.scopedLive.layer(BunServices.layer);

const makeRestartHarness = Effect.gen(function* () {
  const completed = yield* Ref.make(0);
  const blocked = yield* Ref.make(0);
  const activityStarted = yield* Deferred.make<boolean>();
  const releaseActivity = yield* Deferred.make<boolean>();
  const capabilities = Layer.merge(
    Layer.succeed(
      WorkflowCapabilityExecutor,
      WorkflowCapabilityExecutor.of({
        supports: () => true,
        execute: (call) => {
          if (call.stepId === "completed") {
            return Ref.update(completed, (count) => count + 1).pipe(
              Effect.as(
                WorkflowStepExecution.make({ value: call.input, tokenCount: 0, agentRuns: 0 }),
              ),
            );
          }
          return Effect.uninterruptibleMask((restore) =>
            Ref.update(blocked, (count) => count + 1).pipe(
              Effect.andThen(Deferred.succeed(activityStarted, true)),
              Effect.andThen(restore(Deferred.await(releaseActivity))),
              Effect.as(
                WorkflowStepExecution.make({ value: call.input, tokenCount: 0, agentRuns: 0 }),
              ),
            ),
          );
        },
        compensate: () => Effect.void,
      }),
    ),
    Layer.succeed(
      WorkflowArtifactStore,
      WorkflowArtifactStore.of({
        store: () =>
          Effect.succeed(WorkflowArtifactReference.make({ artifactId: ArtifactId.make("unused") })),
      }),
    ),
  );
  return { activityStarted, blocked, capabilities, completed, releaseActivity };
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
      yield* withClient(
        workspaceRoot,
        socketPath,
        LoomClient.pipe(Effect.flatMap((client) => client.handshake)),
      );
      yield* withClient(
        workspaceRoot,
        socketPath,
        LoomClient.pipe(Effect.flatMap((client) => client.startWorkflow(request))),
      );
      yield* Deferred.await(activityStarted);
      yield* Fiber.interrupt(firstDaemon);

      const secondDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
      yield* withClient(
        workspaceRoot,
        socketPath,
        LoomClient.pipe(Effect.flatMap((client) => client.handshake)),
      );
      yield* Deferred.succeed(releaseActivity, true);
      const result = yield* withClient(
        workspaceRoot,
        socketPath,
        LoomClient.pipe(Effect.flatMap((client) => client.executeWorkflow(request))),
      );

      expect(result).toEqual({ completed: "completed", resumed: "resumed" });
      expect(yield* Ref.get(completed)).toBe(1);
      expect(yield* Ref.get(blocked)).toBe(2);
      yield* Fiber.interrupt(secondDaemon);
    }),
  30_000,
);
