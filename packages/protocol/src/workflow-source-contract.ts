import { WorkflowStepId } from "@cvr/loom-domain";
import { Schema } from "effect";
import {
  workflowAgentCapability,
  WorkflowAgentInput,
  workflowArtifactCapability,
  workflowJobCapability,
  WorkflowJobInput,
} from "./workflow-capability-model.js";
import { WorkflowStepCall } from "./workflow-interpreter-model.js";

const encodeStep = Schema.encodeSync(Schema.fromJsonString(WorkflowStepCall));

const jobStep = encodeStep(
  WorkflowStepCall.make({
    stepId: WorkflowStepId.make("validate-report"),
    capability: workflowJobCapability,
    input: WorkflowJobInput.make({ command: "test -f report.txt" }),
  }),
);

const agentStep = encodeStep(
  WorkflowStepCall.make({
    stepId: WorkflowStepId.make("review-report"),
    capability: workflowAgentCapability,
    input: WorkflowAgentInput.make({ prompt: "Review report.txt." }),
  }),
);

export const workflowSourceGuide = [
  "Write an async function body. Do not export a function.",
  'Read the Workflow input from the global "input".',
  "Use one unique stepId for each Step in a Workflow pass.",
  `Declare "${workflowJobCapability}" in capabilities. Run a Job with: await step.run(${jobStep})`,
  `Declare "${workflowAgentCapability}" in capabilities. Run an Agent with: await step.run(${agentStep})`,
  `Declare "${workflowArtifactCapability}" in capabilities to store an oversized Step result. Loom stores it automatically.`,
  'Declare "approval" in signals. Wait for it with: await signal.wait("approval")',
  "Return a JSON value.",
].join("\n");

export const workflowCapabilitiesGuide = `Declare every capability used by source. Built-ins are "${workflowJobCapability}", "${workflowAgentCapability}", and "${workflowArtifactCapability}".`;

export const workflowSignalsGuide =
  "Declare every Signal name used by source. Source waits with signal.wait(name).";

export const describeWorkflowSourceError = (message: string) =>
  `${message}\n\nSupported Workflow source API:\n${workflowSourceGuide}`;
