import { LoomClient } from "@cvr/loom-client";
import {
  SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  WorkflowRunRequest,
  WorkflowVersion,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import { layerNodeLoomClient, layerNodeServices, startNodeDaemon } from "@cvr/loom-platform-node";
import { workflowInterpreterVersion } from "@cvr/loom-protocol";
import { Type, type Static } from "@earendil-works/pi-ai";
import type {
  AgentToolResult,
  ExtensionAPI,
  ExtensionContext,
  SessionStartEvent,
} from "@earendil-works/pi-coding-agent";
import { Duration, Effect, FileSystem, Option, Path, Schema } from "effect";

export interface LoomDaemonStatus {
  readonly started: boolean;
  readonly protocolVersion: number;
  readonly socketPath: string;
}

export interface LoomExtensionApi {
  readonly on: (
    event: "session_start",
    handler: (event: SessionStartEvent, context: ExtensionContext) => Promise<void> | void,
  ) => void;
  readonly registerCommand: ExtensionAPI["registerCommand"];
  readonly registerTool: ExtensionAPI["registerTool"];
}

const daemonEntry = new URL("../../../apps/daemon/src/main.ts", import.meta.url).pathname;

const connect = (
  workspaceRoot: WorkspaceRoot,
  socketPath: string,
  connectionTimeout: Duration.Input,
) =>
  Effect.gen(function* () {
    const client = yield* LoomClient;
    return yield* client.handshake;
  }).pipe(
    Effect.provide(
      layerNodeLoomClient({
        workspaceRoot,
        socketPath,
        connectionTimeout,
      }),
    ),
  );

const executeWorkflow = (cwd: string, request: WorkflowRunRequest) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const workspaceRoot = WorkspaceRoot.make(yield* fs.realPath(path.resolve(cwd)));
    const status = yield* ensureLoomDaemon(cwd);
    return yield* Effect.gen(function* () {
      const client = yield* LoomClient;
      return yield* client.executeWorkflow(request);
    }).pipe(
      Effect.provide(
        layerNodeLoomClient({
          workspaceRoot,
          socketPath: status.socketPath,
          connectionTimeout: Option.match(request.budget.maxDurationMillis, {
            onNone: () => "5 minutes",
            onSome: (milliseconds) => Duration.millis(milliseconds + 5_000),
          }),
        }),
      ),
    );
  }).pipe(Effect.provide(layerNodeServices));

const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json));
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Json));

const workflowParameters = Type.Object({
  name: Type.String({ minLength: 1, description: "Stable workflow name" }),
  version: Type.String({ minLength: 1, description: "Workflow definition version" }),
  key: Type.String({ minLength: 1, description: "Run key within this Pi session" }),
  source: Type.String({
    minLength: 1,
    description: "Async workflow body. Use input and step.run for declared capabilities.",
  }),
  input: Type.String({ description: "Workflow input as JSON text" }),
  capabilities: Type.Optional(Type.Array(Type.String({ minLength: 1 }))),
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

const workflowRequest = (
  sessionId: string,
  parameters: Static<typeof workflowParameters>,
): Effect.Effect<WorkflowRunRequest, Schema.SchemaError> =>
  Effect.map(decodeJson(parameters.input), (input) =>
    WorkflowRunRequest.make({
      sessionId: SessionId.make(sessionId),
      key: WorkflowKey.make(parameters.key),
      definition: WorkflowDefinition.make({
        name: WorkflowName.make(parameters.name),
        version: WorkflowVersion.make(parameters.version),
        interpreterVersion: workflowInterpreterVersion,
        source: parameters.source,
        capabilities: (parameters.capabilities ?? []).map((name) => WorkflowCapability.make(name)),
        signals: [],
      }),
      input,
      budget: WorkflowBudget.make({
        maxSteps: parameters.budget?.maxSteps ?? 32,
        maxAgentRuns: parameters.budget?.maxAgentRuns ?? 8,
        maxParallelism: parameters.budget?.maxParallelism ?? 4,
        maxInlineStepResultBytes: parameters.budget?.maxInlineStepResultBytes ?? 64 * 1_024,
        maxTokens: Option.fromNullishOr(parameters.budget?.maxTokens),
        maxDurationMillis: Option.fromNullishOr(parameters.budget?.maxDurationMillis),
      }),
    }),
  );

const startDaemon = Effect.fn("LoomPiExtension.startDaemon")(function* (
  workspaceRoot: WorkspaceRoot,
) {
  yield* startNodeDaemon({ entryPath: daemonEntry, workspaceRoot });
});

export const ensureLoomDaemon = (cwd: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const workspaceRoot = WorkspaceRoot.make(yield* fs.realPath(path.resolve(cwd)));
    const socketPath = `${workspaceRoot}/.loom/daemon.sock`;
    return yield* connect(workspaceRoot, socketPath, "100 millis").pipe(
      Effect.map((handshake) => ({
        started: false,
        protocolVersion: handshake.protocolVersion,
        socketPath,
      })),
      Effect.catchTag("DaemonUnavailableError", () =>
        startDaemon(workspaceRoot).pipe(
          Effect.flatMap(() => connect(workspaceRoot, socketPath, "5 seconds")),
          Effect.map((handshake) => ({
            started: true,
            protocolVersion: handshake.protocolVersion,
            socketPath,
          })),
        ),
      ),
    );
  }).pipe(Effect.provide(layerNodeServices));

export type EnsureLoomDaemon = typeof ensureLoomDaemon;

const notifyFailure = (context: ExtensionContext, cause: unknown) =>
  Effect.sync(() => context.ui.notify(`Loom daemon failed: ${String(cause)}`, "error"));

const ensureForSession = (context: ExtensionContext, ensureDaemon: EnsureLoomDaemon) =>
  ensureDaemon(context.cwd).pipe(
    Effect.matchEffect({
      onFailure: (cause) => notifyFailure(context, cause),
      onSuccess: (status) => {
        if (!status.started) return Effect.void;
        return Effect.sync(() => context.ui.notify("Loom daemon started.", "info"));
      },
    }),
    Effect.runPromise,
  );

export const registerLoomExtension = (
  pi: LoomExtensionApi,
  ensureDaemon: EnsureLoomDaemon = ensureLoomDaemon,
): void => {
  pi.on("session_start", (_event, context) => ensureForSession(context, ensureDaemon));
  pi.registerCommand("loom", {
    description: "Show the Loom daemon state",
    handler: (_arguments, context) =>
      ensureDaemon(context.cwd).pipe(
        Effect.matchEffect({
          onFailure: (cause) => notifyFailure(context, cause),
          onSuccess: (status) =>
            Effect.sync(() =>
              context.ui.notify(
                `Loom daemon ready. Protocol ${status.protocolVersion}. Socket ${status.socketPath}`,
                "info",
              ),
            ),
        }),
        Effect.runPromise,
      ),
  });
  pi.registerTool({
    name: "loom_workflow",
    label: "Loom Workflow",
    description: "Run a durable workflow in the Loom daemon.",
    promptSnippet: "Run durable replay-safe work in Loom",
    promptGuidelines: [
      "Use loom_workflow when work needs a durable identity or must survive daemon restart.",
    ],
    parameters: workflowParameters,
    execute: (_toolCallId, parameters, signal, _onUpdate, context) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const request = yield* workflowRequest(context.sessionManager.getSessionId(), parameters);
          const result = yield* executeWorkflow(context.cwd, request);
          return {
            content: [{ type: "text", text: encodeJson(result) }],
            details: { result },
          } satisfies AgentToolResult<{ readonly result: Schema.Json }>;
        }),
        { signal },
      ),
  });
};

export default function loomExtension(pi: LoomExtensionApi): void {
  registerLoomExtension(pi);
}
