import { BunServices } from "@effect/platform-bun";
import { LoomClient, type LoomClientShape } from "@cvr/loom-client";
import { ArtifactId, type WorkflowRunAddress, type WorkspaceRoot } from "@cvr/loom-domain";
import { layerBunLoomClient } from "@cvr/loom-platform-bun";
import { WorkflowRunState } from "@cvr/loom-protocol";
import {
  WorkflowArtifactReference,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  type WorkflowCapabilityExecutorShape,
} from "@cvr/loom-runtime";
import { it } from "effect-bun-test";
import { Effect, Layer, Schedule } from "effect";

export const withClient = <A, E, R>(
  workspaceRoot: WorkspaceRoot,
  socketPath: string,
  use: (client: LoomClientShape) => Effect.Effect<A, E, R>,
) =>
  LoomClient.pipe(
    Effect.flatMap(use),
    Effect.provide(
      layerBunLoomClient({ workspaceRoot, socketPath, connectionTimeout: "10 seconds" }),
    ),
  );

export const scopedLive = it.scopedLive.layer(BunServices.layer);

export const waitForSuspension = (
  workspaceRoot: WorkspaceRoot,
  socketPath: string,
  address: WorkflowRunAddress,
) =>
  withClient(workspaceRoot, socketPath, (client) => client.inspectWorkflow(address)).pipe(
    Effect.repeat({
      while: WorkflowRunState.guards.Pending,
      schedule: Schedule.spaced("10 millis"),
    }),
  );

const artifactStore = Layer.succeed(
  WorkflowArtifactStore,
  WorkflowArtifactStore.of({
    store: () =>
      Effect.succeed(WorkflowArtifactReference.make({ artifactId: ArtifactId.make("unused") })),
  }),
);

export const testCapabilities = (executor: WorkflowCapabilityExecutorShape) =>
  Layer.merge(
    Layer.succeed(WorkflowCapabilityExecutor, WorkflowCapabilityExecutor.of(executor)),
    artifactStore,
  );
