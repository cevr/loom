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
  execute: WorkflowInterpreterHost<never>["execute"],
): WorkflowInterpreterHost<never> => ({
  activity: (_stepId, effect) => effect,
  supports: () => true,
  execute,
  storeArtifact: ({ stepId }) =>
    Effect.succeed(
      WorkflowArtifactReference.make({
        artifactId: ArtifactId.make(`artifact-${stepId}`),
      }),
    ),
  withDurationLimit: (_milliseconds, evaluation) => evaluation,
});
