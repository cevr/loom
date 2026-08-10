import {
  LoomDynamicWorkflow,
  loomWorkflowSignal,
  WorkflowArtifactStore,
  WorkflowBudgetExceededError,
  WorkflowCapabilityExecutor,
  layerWorkflowRunAcceptance,
  layerWorkflowRuntime,
  WorkflowRunError,
  WorkflowStepExecution,
} from "@cvr/loom-runtime";
import { WorkflowRunId } from "@cvr/loom-domain";
import { Effect, Layer, Schema } from "effect";
import { ClusterWorkflowEngine } from "effect/unstable/cluster";
import type { WorkflowEngine, WorkflowInstance } from "effect/unstable/workflow/WorkflowEngine";
import { Actor } from "effect-encore";
import { layerSqliteWorkflowRunAcceptanceStore } from "./sqlite-workflow-run-acceptance-store.js";
import { layerSqliteWorkflowSignalDeclarations } from "./sqlite-workflow-signal-declarations.js";
import { interpretWorkflow } from "./workflow-interpreter.js";

const durationStepId = "loom/workflow-duration";
const durationTimerStepId = "loom/workflow-duration/timer";
const DurationOutcome = Schema.TaggedUnion({
  Completed: { value: Schema.Json },
  TimedOut: {},
});

export const layerLoomDynamicWorkflow = Actor.toLayer(LoomDynamicWorkflow, (request, step) =>
  Effect.gen(function* () {
    const capabilities = yield* WorkflowCapabilityExecutor;
    const artifacts = yield* WorkflowArtifactStore;
    return yield* interpretWorkflow<WorkflowEngine | WorkflowInstance>(request, {
      workflowRunId: WorkflowRunId.make(step.executionId),
      activity: (stepId, execute) =>
        step.run(stepId, {
          do: execute,
          success: WorkflowStepExecution,
          error: WorkflowRunError,
        }),
      execute: capabilities.execute,
      supports: capabilities.supports,
      storeArtifact: artifacts.store,
      awaitSignal: (name) => loomWorkflowSignal(name).await,
      withDurationLimit: (milliseconds, evaluation) =>
        Effect.gen(function* () {
          const outcome = yield* step.raceSignals(durationStepId, {
            success: DurationOutcome,
            error: WorkflowRunError,
            effects: [
              evaluation.pipe(
                Effect.map((value) => DurationOutcome.cases.Completed.make({ value })),
              ),
              step
                .sleep(durationTimerStepId, milliseconds)
                .pipe(Effect.as(DurationOutcome.cases.TimedOut.make({}))),
            ],
          });
          if (DurationOutcome.guards.Completed(outcome)) return outcome.value;
          return yield* new WorkflowBudgetExceededError({
            budget: "Duration",
            limit: milliseconds,
            actual: milliseconds,
          });
        }),
    });
  }),
);

export const layerLoomWorkflowRuntime = (() => {
  const engine = ClusterWorkflowEngine.layer;
  const acceptance = layerWorkflowRunAcceptance.pipe(
    Layer.provide(layerSqliteWorkflowRunAcceptanceStore),
  );
  const workflow = layerLoomDynamicWorkflow.pipe(Layer.provideMerge(engine));
  return layerWorkflowRuntime.pipe(
    Layer.provide([workflow, acceptance, layerSqliteWorkflowSignalDeclarations]),
  );
})();
