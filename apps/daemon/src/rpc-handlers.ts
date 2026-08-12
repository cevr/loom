import {
  LoomRpcs,
  PluginStateStoreError,
  type WritePluginStateRequest,
  WorkflowRunAcceptanceError,
  WorkflowRunHandle,
} from "@cvr/loom-protocol";
import { PluginStateScope } from "@cvr/loom-domain";
import {
  AgentActor,
  ActorStateHub,
  ConnectionHandshake,
  JobRuntime,
  PluginStateStore,
  SessionLifecycle,
  WorkflowRuntime,
  type PluginStateStoreShape,
  type SessionLifecycleShape,
} from "@cvr/loom-runtime";
import { Effect, Inspectable, Stream } from "effect";
import { makeCloseSession } from "./close-session.js";
import { makeJobRpcHandlers } from "./job-rpc-handlers.js";

const writePluginState = (pluginState: PluginStateStoreShape, sessions: SessionLifecycleShape) =>
  Effect.fn("LoomRpcHandlers.writePluginState")(function* (request: WritePluginStateRequest) {
    const write = pluginState.write(request.address, request.expected, request.value);
    if (PluginStateScope.guards.Workspace(request.address.scope)) return yield* write;
    return yield* sessions.admit(request.address.scope.sessionId, write).pipe(
      Effect.catchTag(
        "SessionClosureStoreError",
        (error) =>
          new PluginStateStoreError({
            operation: "write",
            message: Inspectable.toStringUnknown(error.cause),
          }),
      ),
    );
  });

export const layerLoomRpcHandlers = LoomRpcs.toLayer(
  Effect.gen(function* () {
    const connection = yield* ConnectionHandshake;
    const actors = yield* ActorStateHub;
    const workflows = yield* WorkflowRuntime;
    const jobs = yield* JobRuntime;
    const pluginState = yield* PluginStateStore;
    const sessions = yield* SessionLifecycle;
    const closeSession = yield* makeCloseSession;
    const writeState = writePluginState(pluginState, sessions);
    return LoomRpcs.of({
      ...makeJobRpcHandlers(jobs, sessions),
      "Connection.Handshake": connection.handshake,
      "Session.Close": ({ sessionId }) => closeSession(sessionId),
      "ActorState.Watch": ({ sessionId }) =>
        actors.snapshots.pipe(
          Stream.map((snapshot) =>
            Array.from(snapshot.values()).filter(
              (projection) => projection.subject.sessionId === sessionId,
            ),
          ),
        ),
      "CodeKernel.EvaluateCell": AgentActor.EvaluateCell.execute,
      "CodeKernel.Reset": AgentActor.ResetCodeKernel.execute,
      "Workflow.Start": (request) =>
        workflows
          .send(request)
          .pipe(Effect.map((workflowRunId) => WorkflowRunHandle.make({ workflowRunId }))),
      "Workflow.Execute": workflows.execute,
      "Workflow.Signal": (request) =>
        sessions.admit(request.address.sessionId, workflows.signal(request)).pipe(
          Effect.catchTag(
            "SessionClosureStoreError",
            (error) =>
              new WorkflowRunAcceptanceError({
                operation: "lookup",
                message: Inspectable.toStringUnknown(error.cause),
              }),
          ),
        ),
      "Workflow.Inspect": workflows.inspect,
      "Workflow.Interrupt": workflows.interrupt,
      "Workflow.DecideCompensation": workflows.decideCompensation,
      "PluginState.Read": ({ address }) => pluginState.read(address),
      "PluginState.Write": writeState,
    });
  }),
);
