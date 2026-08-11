import {
  type WorkflowRunId,
  type WorkflowRunRequest,
  WorkflowActivityKey,
  WorkflowStepId,
} from "@cvr/loom-domain";
import {
  loomWorkflowSignal,
  type WorkflowArtifactStore,
  WorkflowActivityContext,
  WorkflowBudgetExceededError,
  type WorkflowCapabilityExecutor,
  WorkflowRunError,
  WorkflowStepExecution,
} from "@cvr/loom-runtime";
import { Effect, Option, Schema } from "effect";
import { DurableClock } from "effect/unstable/workflow";
import { WorkflowEngine, WorkflowInstance } from "effect/unstable/workflow/WorkflowEngine";
import type { WorkflowStepContext } from "effect-encore";
import type { WorkflowInterpreterHost } from "./workflow-interpreter.js";

type Host = WorkflowInterpreterHost<WorkflowEngine | WorkflowInstance>;
type Step = WorkflowStepContext<typeof WorkflowRunError>;

const durationTimerStepId = "loom/workflow-duration/timer";
const encodeParallelStepIds = Schema.encodeSync(
  Schema.fromJsonString(Schema.Array(WorkflowStepId)),
);

const activityContext = (
  request: WorkflowRunRequest,
  workflowRunId: WorkflowRunId,
  activityKey: string,
) =>
  WorkflowActivityContext.make({
    activityKey: WorkflowActivityKey.make(activityKey),
    sessionId: request.sessionId,
    workflowRunId,
  });

const makeActivity =
  (request: WorkflowRunRequest, workflowRunId: WorkflowRunId, step: Step): Host["activity"] =>
  (stepId, execute, compensate) =>
    Effect.gen(function* () {
      const context = activityContext(request, workflowRunId, yield* step.idempotencyKey(stepId));
      return yield* step.run(stepId, {
        do: execute(context),
        undo: () => compensate(context),
        success: WorkflowStepExecution,
        error: WorkflowRunError,
      });
    });

const makeParallel =
  (request: WorkflowRunRequest, workflowRunId: WorkflowRunId, step: Step): Host["parallel"] =>
  (calls, execute) =>
    Effect.gen(function* () {
      const batchKey = yield* step.idempotencyKey(
        `loom/parallel/${encodeParallelStepIds(calls.map((call) => call.stepId))}`,
      );
      const batchId = `loom/parallel/${batchKey}`;
      const entries = calls.map((call, index) => ({
        call,
        context: activityContext(request, workflowRunId, `${batchKey}/${index}`),
      }));
      return yield* step.run(batchId, {
        do: Effect.gen(function* () {
          const exits = yield* Effect.forEach(
            entries,
            ({ call, context }) => Effect.exit(execute(call, context)),
            { concurrency: "unbounded" },
          );
          return yield* Effect.all(exits);
        }),
        success: Schema.Array(WorkflowStepExecution),
        error: WorkflowRunError,
      });
    });

const withDurationLimit: Host["withDurationLimit"] = (milliseconds, evaluation) =>
  Effect.gen(function* () {
    const engine = yield* WorkflowEngine;
    const instance = yield* WorkflowInstance;
    const clock = DurableClock.make({ name: durationTimerStepId, duration: milliseconds });
    const completed = yield* engine.deferredResult(clock.deferred);
    if (Option.isNone(completed)) {
      yield* engine.scheduleClock(instance.workflow, {
        executionId: instance.executionId,
        clock,
      });
      return yield* evaluation;
    }
    return yield* new WorkflowBudgetExceededError({
      budget: "Duration",
      limit: milliseconds,
      actual: milliseconds,
    });
  });

export const makeWorkflowInterpreterHost = (
  request: WorkflowRunRequest,
  workflowRunId: WorkflowRunId,
  step: Step,
  capabilities: WorkflowCapabilityExecutor["Service"],
  artifacts: WorkflowArtifactStore["Service"],
): Host => ({
  workflowRunId,
  activity: makeActivity(request, workflowRunId, step),
  parallel: makeParallel(request, workflowRunId, step),
  execute: capabilities.execute,
  compensate: capabilities.compensate,
  supports: capabilities.supports,
  storeArtifact: artifacts.store,
  awaitSignal: (name) => loomWorkflowSignal(name).await,
  withDurationLimit,
});
