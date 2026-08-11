import {
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  WorkflowRunRequest,
  WorkflowSignalName,
  WorkflowVersion,
} from "@cvr/loom-domain";
import { workflowInterpreterVersion } from "@cvr/loom-protocol";
import { Option } from "effect";

export const activityRestartRequest = WorkflowRunRequest.make({
  sessionId: SessionId.make("daemon-restart-session"),
  key: WorkflowKey.make("activity-restart"),
  definition: WorkflowDefinition.make({
    name: WorkflowName.make("daemon-restart"),
    version: WorkflowVersion.make("1"),
    interpreterVersion: workflowInterpreterVersion,
    source: `
      const completed = await step.run({
        stepId: "completed",
        capability: "test",
        input: "completed",
      })
      const resumed = await step.run({
        stepId: "blocked",
        capability: "test",
        input: "resumed",
      })
      return { completed, resumed }
    `,
    capabilities: [WorkflowCapability.make("test")],
    signals: [],
  }),
  input: {},
  budget: WorkflowBudget.make({
    maxSteps: 2,
    maxAgentRuns: 1,
    maxParallelism: 1,
    maxInlineStepResultBytes: 1_024,
    maxTokens: Option.none(),
    maxDurationMillis: Option.none(),
  }),
});

export const compensationRestartRequest = WorkflowRunRequest.make({
  ...activityRestartRequest,
  key: WorkflowKey.make("compensation-restart"),
  definition: WorkflowDefinition.make({
    ...activityRestartRequest.definition,
    name: WorkflowName.make("compensation-restart"),
    source: `
      await step.run({
        stepId: "blocked-compensation",
        capability: "test",
        input: "blocked",
      })
      await step.run({
        stepId: "completed-compensation",
        capability: "test",
        input: "completed",
      })
      return await step.run({
        stepId: "failed",
        capability: "test",
        input: "failed",
      })
    `,
  }),
  budget: WorkflowBudget.make({ ...activityRestartRequest.budget, maxSteps: 3 }),
});

export const signalRestartRequest = WorkflowRunRequest.make({
  ...activityRestartRequest,
  key: WorkflowKey.make("signal-restart"),
  definition: WorkflowDefinition.make({
    ...activityRestartRequest.definition,
    name: WorkflowName.make("signal-restart"),
    source: `
      const completed = await step.run({
        stepId: "completed",
        capability: "test",
        input: "completed",
      })
      const received = await signal.wait("continue")
      const resumed = await step.run({
        stepId: "resumed",
        capability: "test",
        input: received,
      })
      return { completed, signal: received, resumed }
    `,
    signals: [WorkflowSignalName.make("continue")],
  }),
});
