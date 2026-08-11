import {
  LoomDynamicWorkflow,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  layerWorkflowRunAcceptance,
  layerWorkflowRunRecovery,
  layerWorkflowRunStatePublisher,
  layerWorkflowRuntime,
  type WorkflowRunStatePublisherOptions,
} from "@cvr/loom-runtime";
import { WorkflowRunId } from "@cvr/loom-domain";
import { Effect, Layer } from "effect";
import { ClusterWorkflowEngine } from "effect/unstable/cluster";
import { WorkflowEngine, WorkflowInstance } from "effect/unstable/workflow/WorkflowEngine";
import { Actor, ClientLayer } from "effect-encore";
import { layerSqliteWorkflowRunAcceptanceStore } from "./sqlite-workflow-run-acceptance-store.js";
import { layerSqliteWorkflowRunRetention } from "./sqlite-workflow-run-retention.js";
import { layerSqliteWorkflowSignalDeclarations } from "./sqlite-workflow-signal-declarations.js";
import { interpretWorkflow, makeWorkflowInterpreterHost } from "./workflow-interpreter.js";

export const layerLoomDynamicWorkflow = Actor.toLayer(LoomDynamicWorkflow, (execution, step) =>
  Effect.gen(function* () {
    const { request } = execution;
    const workflowRunId = WorkflowRunId.make(step.executionId);
    const capabilities = yield* WorkflowCapabilityExecutor;
    const artifacts = yield* WorkflowArtifactStore;
    return yield* interpretWorkflow<WorkflowEngine | WorkflowInstance>(
      request,
      makeWorkflowInterpreterHost(request, workflowRunId, step, capabilities, artifacts),
    );
  }),
);

export const layerLoomWorkflowRuntimeWith = (options?: WorkflowRunStatePublisherOptions) => {
  const engine = ClusterWorkflowEngine.layer;
  const acceptanceStore = layerSqliteWorkflowRunAcceptanceStore;
  const acceptance = layerWorkflowRunAcceptance.pipe(Layer.provide(acceptanceStore));
  const retention = layerSqliteWorkflowRunRetention.pipe(
    Layer.provideMerge(ClientLayer.fromSharding),
    Layer.provide(acceptanceStore),
  );
  const workflow = Layer.merge(
    layerLoomDynamicWorkflow,
    layerWorkflowRunStatePublisher(options),
  ).pipe(Layer.provideMerge([engine, retention]));
  return Layer.merge(layerWorkflowRuntime, layerWorkflowRunRecovery).pipe(
    Layer.provide([workflow, acceptance, layerSqliteWorkflowSignalDeclarations]),
  );
};

export const layerLoomWorkflowRuntime = layerLoomWorkflowRuntimeWith();
