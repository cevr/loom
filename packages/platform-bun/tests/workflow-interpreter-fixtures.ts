import {
  ArtifactId,
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  WorkflowActivityKey,
  WorkflowRunId,
  WorkflowRunRequest,
  WorkflowVersion,
} from "@cvr/loom-domain";
import { WorkflowArtifactReference, WorkflowStepExecution } from "@cvr/loom-runtime";
import { workflowInterpreterVersion } from "@cvr/loom-protocol";
import { Effect, Option } from "effect";
import type { WorkflowInterpreterHost } from "../src/index.js";

export const budget = WorkflowBudget.make({
  maxSteps: 8,
  maxAgentRuns: 4,
  maxParallelism: 2,
  maxInlineStepResultBytes: 1_024,
  maxTokens: Option.some(1_000),
  maxDurationMillis: Option.none(),
});

export const request = (
  source: string,
  capabilities: ReadonlyArray<string> = ["echo"],
  acceptedBudget: WorkflowBudget = budget,
  interpreterVersion = workflowInterpreterVersion,
): WorkflowRunRequest =>
  WorkflowRunRequest.make({
    sessionId: SessionId.make("session-1"),
    key: WorkflowKey.make("run-1"),
    definition: WorkflowDefinition.make({
      name: WorkflowName.make("test"),
      version: WorkflowVersion.make("1"),
      interpreterVersion,
      source,
      capabilities: capabilities.map((capability) => WorkflowCapability.make(capability)),
      signals: [],
    }),
    input: { value: 42 },
    budget: acceptedBudget,
  });

export const execution = (value: WorkflowStepExecution["value"]): WorkflowStepExecution =>
  WorkflowStepExecution.make({ value, tokenCount: 0, agentRuns: 0 });

export const host = (
  executeStep: WorkflowInterpreterHost<never>["execute"],
): WorkflowInterpreterHost<never> => ({
  workflowRunId: WorkflowRunId.make("workflow-run-1"),
  activity: (_stepId, effect) =>
    effect({
      activityKey: WorkflowActivityKey.make("activity-1"),
      sessionId: SessionId.make("session-1"),
      workflowRunId: WorkflowRunId.make("workflow-run-1"),
    }),
  parallel: (calls, execute) =>
    Effect.forEach(
      calls,
      (call, index) =>
        execute(call, {
          activityKey: WorkflowActivityKey.make(`parallel-${index}`),
          sessionId: SessionId.make("session-1"),
          workflowRunId: WorkflowRunId.make("workflow-run-1"),
        }),
      { concurrency: "unbounded" },
    ),
  awaitSignal: () => Effect.succeed({ received: true }),
  supports: () => true,
  execute: executeStep,
  compensate: () => Effect.void,
  storeArtifact: ({ stepId }) =>
    Effect.succeed(
      WorkflowArtifactReference.make({
        artifactId: ArtifactId.make(`artifact-${stepId}`),
      }),
    ),
  withDurationLimit: (_milliseconds, evaluation) => evaluation,
});
