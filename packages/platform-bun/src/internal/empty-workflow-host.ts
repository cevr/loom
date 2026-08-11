import { WorkflowCapability, workflowArtifactId } from "@cvr/loom-domain";
import {
  WorkflowArtifactStore,
  WorkflowArtifactStoreError,
  WorkflowCapabilityExecutor,
} from "@cvr/loom-runtime";
import { WorkflowStepError } from "@cvr/loom-protocol";
import { Effect, Layer } from "effect";

const unsupported = (stepId: WorkflowStepError["stepId"], capability: WorkflowCapability) =>
  new WorkflowStepError({
    stepId,
    capability,
    message: `No adapter is installed for the ${capability} workflow capability.`,
  });

export const layerEmptyWorkflowHost = Layer.merge(
  Layer.succeed(
    WorkflowCapabilityExecutor,
    WorkflowCapabilityExecutor.of({
      supports: () => false,
      execute: (call) => Effect.fail(unsupported(call.stepId, call.capability)),
      compensate: () => Effect.void,
    }),
  ),
  Layer.succeed(
    WorkflowArtifactStore,
    WorkflowArtifactStore.of({
      store: (_write, context) =>
        Effect.fail(
          new WorkflowArtifactStoreError({
            artifactId: workflowArtifactId(context.activityKey),
            cause: "No Artifact store is installed.",
          }),
        ),
      read: (reference) =>
        Effect.fail(
          new WorkflowArtifactStoreError({
            artifactId: reference.artifactId,
            cause: "No Artifact store is installed.",
          }),
        ),
    }),
  ),
);
