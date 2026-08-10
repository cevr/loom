import { BunServices } from "@effect/platform-bun";
import { LoomClient, type LoomClientShape } from "@cvr/loom-client";
import { ArtifactId, type WorkspaceRoot } from "@cvr/loom-domain";
import { layerBunLoomClient } from "@cvr/loom-platform-bun";
import {
  WorkflowArtifactReference,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  type WorkflowCapabilityExecutorShape,
} from "@cvr/loom-runtime";
import { it } from "effect-bun-test";
import { Effect, Layer } from "effect";

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
