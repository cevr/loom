import {
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowIncarnationId,
  WorkflowKey,
  WorkflowName,
  WorkflowRunRequest,
  WorkflowRunExecution,
  WorkflowSignalName,
  WorkflowVersion,
} from "@cvr/loom-domain";
import { workflowInterpreterVersion } from "@cvr/loom-protocol";
import { Option } from "effect";

export const execution = (request: WorkflowRunRequest) =>
  WorkflowRunExecution.make({
    incarnationId: WorkflowIncarnationId.make(`workflow-incarnation-${request.key}`),
    request,
  });

export const request = WorkflowRunRequest.make({
  sessionId: SessionId.make("session-1"),
  key: WorkflowKey.make("shared"),
  definition: WorkflowDefinition.make({
    name: WorkflowName.make("echo"),
    version: WorkflowVersion.make("1"),
    interpreterVersion: workflowInterpreterVersion,
    source: `
      return await step.run({
        stepId: "echo",
        capability: "echo",
        input,
      })
    `,
    capabilities: [WorkflowCapability.make("echo")],
    signals: [],
  }),
  input: { value: 42 },
  budget: WorkflowBudget.make({
    maxSteps: 2,
    maxAgentRuns: 1,
    maxParallelism: 1,
    maxInlineStepResultBytes: 1_024,
    maxTokens: Option.some(1_000),
    maxDurationMillis: Option.none(),
  }),
});

export const durationRequest = WorkflowRunRequest.make({
  ...request,
  key: WorkflowKey.make("duration"),
  definition: WorkflowDefinition.make({
    ...request.definition,
    source: 'return await signal.wait("duration")',
    signals: [WorkflowSignalName.make("duration")],
  }),
  budget: WorkflowBudget.make({
    ...request.budget,
    maxDurationMillis: Option.some(25),
  }),
});

export const signalRequest = WorkflowRunRequest.make({
  ...request,
  key: WorkflowKey.make("signal"),
  definition: WorkflowDefinition.make({
    ...request.definition,
    source: `return await signal.wait("approval")`,
    capabilities: [],
    signals: [WorkflowSignalName.make("approval")],
  }),
});

export const failureRequest = WorkflowRunRequest.make({
  ...request,
  key: WorkflowKey.make("failure"),
  definition: WorkflowDefinition.make({
    ...request.definition,
    source: `throw new Error("failed")`,
    capabilities: [],
  }),
});
