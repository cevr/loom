import { BunServices } from "@effect/platform-bun";
import { LoomClient, MessageTooLargeError } from "@cvr/loom-client";
import { AgentId, CellId, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { LoomRpcs, maximumCellSourceLength, maximumFrameSize } from "@cvr/loom-protocol";
import { makeConnectionHandshake } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, Exit, FileSystem, Layer, Scope } from "effect";
import {
  layerBunLoomClient,
  layerBunLoomServer,
  makeCodeKernel,
  prepareDaemonSocket,
} from "../src/index.js";

const workspaceRoot = WorkspaceRoot.make("/workspace");
const owner = {
  sessionId: SessionId.make("session-1"),
  agentId: AgentId.make("agent-1"),
};

const layerHandlers = (daemonStartedAtMillis: number, expectedRoot = workspaceRoot) =>
  LoomRpcs.toLayer(
    Effect.gen(function* () {
      const connection = makeConnectionHandshake({
        workspaceRoot: expectedRoot,
        daemonStartedAtMillis,
      });
      const kernel = yield* makeCodeKernel;
      return LoomRpcs.of({
        "Connection.Handshake": connection.handshake,
        "CodeKernel.EvaluateCell": (request) =>
          kernel.evaluate({ cellId: request.cellId, source: request.source }),
        "CodeKernel.Reset": () => kernel.reset,
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

it.scoped("connects and evaluates a Cell through the real Unix socket", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-rpc-" });
    const socketPath = `${directory}/daemon.sock`;
    const live = Layer.merge(layerClient(socketPath), layerServer(socketPath, 100));

    yield* Effect.gen(function* () {
      const client = yield* LoomClient;
      const handshake = yield* client.handshake;
      const cell = yield* client.evaluateCell({
        ...owner,
        cellId: CellId.make("cell-1"),
        source: "40 + 2",
      });

      expect(handshake.maximumFrameSize).toBe(maximumFrameSize);
      expect(cell.display).toBe("42");
    }).pipe(Effect.provide(live));
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scoped("rejects a client routed to another Workspace", () =>
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
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("shows a typed failure when the daemon is unavailable", () =>
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
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scoped("rejects an oversized Cell before socket I/O", () =>
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
      Layer.merge(
        layerBunLoomClient({
          socketPath: "/tmp/loom-not-used.sock",
          workspaceRoot,
        }),
        BunServices.layer,
      ),
    ),
  ),
);

it.scopedLive("reconnects after the daemon restarts", () =>
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
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("removes a stale daemon socket path", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-rpc-stale-" });
    const socketPath = `${directory}/daemon.sock`;
    yield* fs.writeFileString(socketPath, "stale");

    yield* prepareDaemonSocket(socketPath);

    expect(yield* fs.exists(socketPath)).toBe(false);
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("rejects a live daemon socket", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-rpc-live-" });
    const socketPath = `${directory}/daemon.sock`;
    yield* Layer.build(layerServer(socketPath, 100));

    const error = yield* prepareDaemonSocket(socketPath).pipe(Effect.flip);

    expect(error).toHaveProperty("_tag", "DaemonAlreadyRunningError");
  }).pipe(Effect.provide(BunServices.layer)),
);
