import { BunServices } from "@effect/platform-bun";
import { LoomClient, MessageTooLargeError } from "@cvr/loom-client";
import {
  AgentId,
  CellId,
  SessionId,
  WorkflowBudget,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  WorkflowRunId,
  WorkflowRunRequest,
  WorkflowSignalAddress,
  WorkflowSignalName,
  WorkflowVersion,
  WorkspaceRoot,
} from "@cvr/loom-domain";
import { LoomRpcs, maximumCellSourceLength, maximumFrameSize } from "@cvr/loom-protocol";
import { makeConnectionHandshake } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, Exit, FileSystem, Layer, Option, Scope } from "effect";
import {
  layerBunLoomClient,
  layerBunLoomServer,
  makeInProcessCodeKernel,
  prepareDaemonSocket,
} from "../src/index.js";

const workspaceRoot = WorkspaceRoot.make("/workspace");
const owner = {
  sessionId: SessionId.make("session-1"),
  agentId: AgentId.make("agent-1"),
};
const workflow = WorkflowRunRequest.make({
  sessionId: owner.sessionId,
  key: WorkflowKey.make("rpc"),
  definition: WorkflowDefinition.make({
    name: WorkflowName.make("rpc"),
    version: WorkflowVersion.make("1"),
    interpreterVersion: 1,
    source: "return input",
    capabilities: [],
    signals: [],
  }),
  input: { value: 42 },
  budget: WorkflowBudget.make({
    maxSteps: 1,
    maxAgentRuns: 1,
    maxParallelism: 1,
    maxInlineStepResultBytes: 1_024,
    maxTokens: Option.none(),
    maxDurationMillis: Option.none(),
  }),
});
const scoped = it.scoped.layer(BunServices.layer);
const scopedLive = it.scopedLive.layer(BunServices.layer);

const layerHandlers = (daemonStartedAtMillis: number, expectedRoot = workspaceRoot) =>
  LoomRpcs.toLayer(
    Effect.gen(function* () {
      const connection = makeConnectionHandshake({
        workspaceRoot: expectedRoot,
        daemonStartedAtMillis,
      });
      const kernel = yield* makeInProcessCodeKernel;
      return LoomRpcs.of({
        "Connection.Handshake": connection.handshake,
        "Session.Close": () => Effect.void,
        "CodeKernel.EvaluateCell": (request) =>
          kernel.evaluate({ cellId: request.cellId, source: request.source }),
        "CodeKernel.Reset": () => kernel.reset,
        "Workflow.Execute": (request) => Effect.succeed(request.input),
        "Workflow.Signal": () => Effect.void,
        "Workflow.Start": () =>
          Effect.succeed({ workflowRunId: WorkflowRunId.make("workflow-run-1") }),
      });
    }),
  );

const layerServer = (socketPath: string, daemonStartedAtMillis: number) =>
  layerBunLoomServer({ socketPath }).pipe(Layer.provide(layerHandlers(daemonStartedAtMillis)));

const layerClient = (socketPath: string, root = workspaceRoot) =>
  layerBunLoomClient({
    socketPath,
    workspaceRoot: root,
    connectionTimeout: "2 seconds",
  });

scoped("calls typed daemon procedures through the real Unix socket", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-rpc-" });
    const socketPath = `${directory}/daemon.sock`;
    const live = Layer.merge(layerClient(socketPath), layerServer(socketPath, 100));

    yield* Effect.gen(function* () {
      const client = yield* LoomClient;
      const handshake = yield* client.handshake;
      yield* client.closeSession(owner.sessionId);
      const cell = yield* client.evaluateCell({
        ...owner,
        cellId: CellId.make("cell-1"),
        source: "40 + 2",
      });
      const workflowResult = yield* client.executeWorkflow(workflow);
      const workflowHandle = yield* client.startWorkflow(workflow);
      yield* client.signalWorkflow({
        address: WorkflowSignalAddress.make({
          workflowRunId: workflowHandle.workflowRunId,
          name: WorkflowSignalName.make("approval"),
        }),
        value: { approved: true },
      });

      expect(handshake.maximumFrameSize).toBe(maximumFrameSize);
      expect(cell.display).toBe("42");
      expect(workflowResult).toEqual(workflow.input);
      expect(workflowHandle.workflowRunId).toBe(WorkflowRunId.make("workflow-run-1"));
    }).pipe(Effect.provide(live));
  }),
);

scoped("rejects a client routed to another Workspace", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-rpc-mismatch-" });
    const socketPath = `${directory}/daemon.sock`;
    const live = Layer.merge(
      layerClient(socketPath, WorkspaceRoot.make("/other")),
      layerServer(socketPath, 100),
    );

    const error = yield* Effect.gen(function* () {
      const client = yield* LoomClient;
      return yield* client.handshake.pipe(Effect.flip);
    }).pipe(Effect.provide(live));

    expect(error).toHaveProperty("_tag", "WorkspaceMismatchError");
  }),
);

scopedLive("shows a typed failure when the daemon is unavailable", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-rpc-missing-" });
    const socketPath = `${directory}/daemon.sock`;
    const error = yield* Effect.gen(function* () {
      const client = yield* LoomClient;
      return yield* client.handshake.pipe(Effect.flip);
    }).pipe(
      Effect.provide(
        layerBunLoomClient({
          socketPath,
          workspaceRoot,
          connectionTimeout: "50 millis",
        }),
      ),
    );

    expect(error).toHaveProperty("_tag", "DaemonUnavailableError");
  }),
);

scoped("rejects an oversized Cell before socket I/O", () =>
  Effect.gen(function* () {
    const client = yield* LoomClient;
    const error = yield* client
      .evaluateCell({
        ...owner,
        cellId: CellId.make("cell-large"),
        source: "x".repeat(maximumCellSourceLength + 1),
      })
      .pipe(Effect.flip);

    expect(error).toBeInstanceOf(MessageTooLargeError);
  }).pipe(
    Effect.provide(
      layerBunLoomClient({
        socketPath: "/tmp/loom-not-used.sock",
        workspaceRoot,
      }),
    ),
  ),
);

scopedLive("reconnects after the daemon restarts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-rpc-reconnect-" });
    const socketPath = `${directory}/daemon.sock`;
    const firstScope = yield* Scope.make();
    yield* Layer.buildWithScope(layerServer(socketPath, 100), firstScope);

    yield* Effect.gen(function* () {
      const client = yield* LoomClient;
      expect((yield* client.handshake).daemonStartedAtMillis).toBe(100);

      yield* Scope.close(firstScope, Exit.void);
      const secondScope = yield* Scope.make();
      yield* Layer.buildWithScope(layerServer(socketPath, 200), secondScope);
      expect((yield* client.handshake).daemonStartedAtMillis).toBe(200);
      yield* Scope.close(secondScope, Exit.void);
    }).pipe(Effect.provide(layerClient(socketPath)));
  }),
);

scopedLive("removes a stale daemon socket path", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-rpc-stale-" });
    const socketPath = `${directory}/daemon.sock`;
    yield* fs.writeFileString(socketPath, "stale");

    yield* prepareDaemonSocket(socketPath);

    expect(yield* fs.exists(socketPath)).toBe(false);
  }),
);

scopedLive("rejects a live daemon socket", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-rpc-live-" });
    const socketPath = `${directory}/daemon.sock`;
    yield* Layer.build(layerServer(socketPath, 100));

    const error = yield* prepareDaemonSocket(socketPath).pipe(Effect.flip);

    expect(error).toHaveProperty("_tag", "DaemonAlreadyRunningError");
  }),
);
