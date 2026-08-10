import { WorkflowCapability } from "@cvr/loom-domain";
import { WorkflowArtifactStore, WorkflowCapabilityExecutor } from "@cvr/loom-runtime";
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
    }),
  ),
  Layer.succeed(
    WorkflowArtifactStore,
    WorkflowArtifactStore.of({
      store: (write) => Effect.fail(unsupported(write.stepId, WorkflowCapability.make("artifact"))),
    }),
  ),
);
