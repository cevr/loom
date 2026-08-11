import {
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  WorkflowRunRequest,
  WorkflowRunId,
  WorkflowSignalName,
  WorkflowVersion,
} from "@cvr/loom-domain";
import {
  WorkflowCompensationDecision,
  workflowInterpreterVersion,
  workflowCapabilitiesGuide,
  workflowSignalsGuide,
  workflowSourceGuide,
} from "@cvr/loom-protocol";
import { StringEnum, Type, type Static } from "@earendil-works/pi-ai";
import { Effect, Schema } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";
import { runWithLoomClient } from "./loom-connection.js";
import { runTool, toolResult } from "./tool-result.js";

const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json));

const workflowParameters = Type.Object({
  name: Type.String({ minLength: 1, description: "Stable workflow name" }),
  version: Type.String({ minLength: 1, description: "Workflow definition version" }),
  key: Type.String({ minLength: 1, description: "Run key within this Pi session" }),
  source: Type.String({
    minLength: 1,
    description: workflowSourceGuide,
  }),
  input: Type.String({ description: "Workflow input as JSON text" }),
  capabilities: Type.Optional(
    Type.Array(Type.String({ minLength: 1 }), { description: workflowCapabilitiesGuide }),
  ),
  signals: Type.Optional(
    Type.Array(Type.String({ minLength: 1 }), { description: workflowSignalsGuide }),
  ),
  budget: Type.Optional(
    Type.Object({
      maxSteps: Type.Optional(Type.Integer({ minimum: 1 })),
      maxAgentRuns: Type.Optional(Type.Integer({ minimum: 1 })),
      maxParallelism: Type.Optional(Type.Integer({ minimum: 1 })),
      maxInlineStepResultBytes: Type.Optional(Type.Integer({ minimum: 1 })),
      maxTokens: Type.Optional(Type.Integer({ minimum: 1 })),
      maxDurationMillis: Type.Optional(Type.Integer({ minimum: 1 })),
    }),
  ),
});

const decodeWorkflowBudget = Schema.decodeUnknownEffect(WorkflowBudget);

export const workflowRequest = (
  sessionId: string,
  parameters: Static<typeof workflowParameters>,
): Effect.Effect<WorkflowRunRequest, Schema.SchemaError> =>
  Effect.gen(function* () {
    const input = yield* decodeJson(parameters.input);
    const budget = yield* decodeWorkflowBudget(parameters.budget ?? {});
    return WorkflowRunRequest.make({
      sessionId: SessionId.make(sessionId),
      key: WorkflowKey.make(parameters.key),
      definition: WorkflowDefinition.make({
        name: WorkflowName.make(parameters.name),
        version: WorkflowVersion.make(parameters.version),
        interpreterVersion: workflowInterpreterVersion,
        source: parameters.source,
        capabilities: (parameters.capabilities ?? []).map((name) => WorkflowCapability.make(name)),
        signals: (parameters.signals ?? []).map((name) => WorkflowSignalName.make(name)),
      }),
      input,
      budget,
    });
  });

const workflowAddressParameters = Type.Object({ workflowRunId: Type.String({ minLength: 1 }) });

const workflowAddress = (sessionId: string, workflowRunId: string) => ({
  sessionId: SessionId.make(sessionId),
  workflowRunId: WorkflowRunId.make(workflowRunId),
});

const registerStartWorkflow = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_workflow_start",
    label: "Start Loom Workflow",
    description: "Start durable replay-safe work and return its Workflow Run ID.",
    promptSnippet: "Start durable replay-safe work through Loom",
    promptGuidelines: [
      "Use loom_workflow_start when work needs a durable identity or must survive daemon restart.",
    ],
    parameters: workflowParameters,
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        Effect.gen(function* () {
          const request = yield* workflowRequest(context.sessionManager.getSessionId(), parameters);
          return yield* runWithLoomClient(context.cwd, "10 seconds", (client) =>
            client.startWorkflow(request).pipe(Effect.map(toolResult)),
          );
        }),
        { signal },
      ),
  });

const registerInspectWorkflow = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_workflow_inspect",
    label: "Inspect Loom Workflow",
    description: "Read the current state of a Workflow Run.",
    parameters: workflowAddressParameters,
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        runWithLoomClient(context.cwd, "5 seconds", (client) =>
          client
            .inspectWorkflow(
              workflowAddress(context.sessionManager.getSessionId(), parameters.workflowRunId),
            )
            .pipe(Effect.map(toolResult)),
        ),
        { signal },
      ),
  });

const workflowSignalParameters = Type.Object({
  workflowRunId: Type.String({ minLength: 1 }),
  name: Type.String({ minLength: 1 }),
  value: Type.String({ description: "Signal value as JSON text" }),
});

const registerSignalWorkflow = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_workflow_signal",
    label: "Signal Loom Workflow",
    description: "Send a declared durable Signal to a Workflow Run.",
    parameters: workflowSignalParameters,
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        Effect.gen(function* () {
          const value = yield* decodeJson(parameters.value);
          return yield* runWithLoomClient(context.cwd, "5 seconds", (client) =>
            client
              .signalWorkflow({
                address: {
                  ...workflowAddress(
                    context.sessionManager.getSessionId(),
                    parameters.workflowRunId,
                  ),
                  name: WorkflowSignalName.make(parameters.name),
                },
                value,
              })
              .pipe(Effect.as(toolResult("Workflow signalled"))),
          );
        }),
        { signal },
      ),
  });

const registerInterruptWorkflow = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_workflow_interrupt",
    label: "Interrupt Loom Workflow",
    description: "Interrupt a Workflow Run.",
    parameters: workflowAddressParameters,
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        runWithLoomClient(context.cwd, "5 seconds", (client) =>
          client
            .interruptWorkflow(
              workflowAddress(context.sessionManager.getSessionId(), parameters.workflowRunId),
            )
            .pipe(Effect.as(toolResult("Workflow interrupted"))),
        ),
        { signal },
      ),
  });

const workflowCompensationParameters = Type.Object({
  workflowRunId: Type.String({ minLength: 1 }),
  decision: StringEnum(["Retry", "Stop"]),
});

const registerCompensationDecision = (pi: LoomExtensionApi) =>
  pi.registerTool({
    name: "loom_workflow_compensation",
    label: "Decide Loom Workflow Compensation",
    description: "Retry or stop a failed Workflow compensation.",
    parameters: workflowCompensationParameters,
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      runTool(
        Effect.gen(function* () {
          const decision = yield* Schema.decodeUnknownEffect(WorkflowCompensationDecision)(
            parameters.decision,
          );
          return yield* runWithLoomClient(context.cwd, "5 seconds", (client) =>
            client
              .decideWorkflowCompensation({
                address: workflowAddress(
                  context.sessionManager.getSessionId(),
                  parameters.workflowRunId,
                ),
                decision,
              })
              .pipe(Effect.as(toolResult("Workflow compensation decision recorded"))),
          );
        }),
        { signal },
      ),
  });

export const registerWorkflowTools = (pi: LoomExtensionApi): void => {
  registerStartWorkflow(pi);
  registerInspectWorkflow(pi);
  registerSignalWorkflow(pi);
  registerInterruptWorkflow(pi);
  registerCompensationDecision(pi);
};
