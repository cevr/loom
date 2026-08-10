import { BunServices } from "@effect/platform-bun";
import { AgentId, CellId, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { layerLoomSqlite, layerSqliteCellLedger } from "@cvr/loom-platform-bun";
import {
  CellEvaluation,
  CellExecutionError,
  CellLedgerEntry,
  CellLedgerState,
} from "@cvr/loom-protocol";
import {
  AgentActor,
  CellLedger,
  CodeKernel,
  CodeKernelFactory,
  layerAgentActor,
  type CodeKernelShape,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Layer, Ref } from "effect";
import { TestRunner } from "effect/unstable/cluster";
import { runLoomDaemon } from "../src/program.js";
import { testCapabilities, withClient } from "./workflow-test-support.js";

const owner = { sessionId: SessionId.make("session-1"), agentId: AgentId.make("agent-1") };
const scopedLive = it.scopedLive.layer(BunServices.layer);

const layerCellActor = (filename: string, kernel: CodeKernelShape) =>
  layerAgentActor.pipe(
    Layer.provide([
      TestRunner.layer,
      layerSqliteCellLedger,
      Layer.succeed(
        CodeKernelFactory,
        CodeKernelFactory.of({ spawn: () => Effect.succeed(kernel) }),
      ),
    ]),
    Layer.provide(layerLoomSqlite({ filename })),
  );

const countedKernel = (cellId: CellId, evaluations: Ref.Ref<number>) =>
  CodeKernel.of({
    evaluate: () =>
      Ref.updateAndGet(evaluations, (count) => count + 1).pipe(
        Effect.map((count) =>
          CellEvaluation.make({ cellId, display: String(count), bindings: [] }),
        ),
      ),
    reset: Effect.void,
    close: Effect.void,
  });

scopedLive("returns one stored Cell outcome after a client retry", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-cell-retry-" });
    const evaluations = yield* Ref.make(0);
    const cellId = CellId.make("cell-retried");
    const live = layerCellActor(`${directory}/loom.sqlite`, countedKernel(cellId, evaluations));
    const request = { ...owner, cellId, source: "externalEffect()" };

    const results = yield* Effect.gen(function* () {
      const first = yield* AgentActor.EvaluateCell.execute(request);
      const retry = yield* AgentActor.EvaluateCell.execute(request);
      return [first, retry];
    }).pipe(Effect.provide(live));

    expect(results.map((result) => result.display)).toEqual(["1", "1"]);
    expect(yield* Ref.get(evaluations)).toBe(1);
  }),
);

scopedLive("rejects one Cell identity with different source", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-cell-conflict-" });
    const evaluations = yield* Ref.make(0);
    const cellId = CellId.make("cell-conflict");
    const live = layerCellActor(`${directory}/loom.sqlite`, countedKernel(cellId, evaluations));

    const failure = yield* Effect.gen(function* () {
      yield* AgentActor.EvaluateCell.execute({ ...owner, cellId, source: "first" });
      return yield* AgentActor.EvaluateCell.execute({ ...owner, cellId, source: "changed" }).pipe(
        Effect.flip,
      );
    }).pipe(Effect.provide(live));

    expect(failure).toHaveProperty("_tag", "CellIdentityConflictError");
    expect(yield* Ref.get(evaluations)).toBe(1);
  }),
);

scopedLive("returns one stored terminal Cell error after a retry", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-cell-error-" });
    const evaluations = yield* Ref.make(0);
    const cellId = CellId.make("cell-failed");
    const kernel = CodeKernel.of({
      evaluate: () =>
        Ref.update(evaluations, (count) => count + 1).pipe(
          Effect.andThen(
            Effect.fail(new CellExecutionError({ cellId, message: "external failure" })),
          ),
        ),
      reset: Effect.void,
      close: Effect.void,
    });
    const live = layerCellActor(`${directory}/loom.sqlite`, kernel);
    const request = { ...owner, cellId, source: "failExternalEffect()" };

    const failures = yield* Effect.gen(function* () {
      const first = yield* AgentActor.EvaluateCell.execute(request).pipe(Effect.flip);
      const retry = yield* AgentActor.EvaluateCell.execute(request).pipe(Effect.flip);
      return [first, retry];
    }).pipe(Effect.provide(live));

    expect(failures[0]).toHaveProperty("_tag", "CellExecutionError");
    expect(failures[1]).toHaveProperty("_tag", "CellExecutionError");
    expect(yield* Ref.get(evaluations)).toBe(1);
  }),
);

scopedLive("does not repeat a Cell effect after an RPC retry or daemon restart", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-cell-restart-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const socketPath = `${directory}/daemon.sock`;
    const effectPath = `${directory}/external-effect`;
    const config = { workspaceRoot, socketPath, databasePath: `${directory}/loom.sqlite` };
    const capabilities = testCapabilities({
      supports: () => false,
      execute: () => Effect.die("No Workflow capability is used by this test."),
      compensate: () => Effect.void,
    });
    const cellId = CellId.make("cell-before-restart");
    const request = {
      ...owner,
      cellId,
      source: `const current = await Bun.file("${effectPath}").text(); await Bun.write("${effectPath}", current + "x"); "done"`,
    };
    yield* fs.writeFileString(effectPath, "");

    const firstDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
    yield* withClient(workspaceRoot, socketPath, (client) => client.handshake);
    const first = yield* withClient(workspaceRoot, socketPath, (client) =>
      client.evaluateCell(request),
    );
    const retry = yield* withClient(workspaceRoot, socketPath, (client) =>
      client.evaluateCell(request),
    );
    const conflict = yield* withClient(workspaceRoot, socketPath, (client) =>
      client.evaluateCell({ ...request, source: `${request.source} ` }).pipe(Effect.flip),
    );
    yield* Fiber.interrupt(firstDaemon);

    const secondDaemon = yield* runLoomDaemon(config, capabilities).pipe(Effect.forkScoped);
    yield* withClient(workspaceRoot, socketPath, (client) => client.handshake);
    const restored = yield* withClient(workspaceRoot, socketPath, (client) =>
      client.evaluateCell(request),
    );

    expect([first.display, retry.display, restored.display]).toEqual([
      '"done"',
      '"done"',
      '"done"',
    ]);
    expect(conflict).toHaveProperty("_tag", "CellIdentityConflictError");
    expect(yield* fs.readFileString(effectPath)).toBe("x");
    yield* Fiber.interrupt(secondDaemon);
  }),
);

scopedLive("interrupts an unfinished Cell before the daemon accepts a retry", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-cell-startup-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const socketPath = `${directory}/daemon.sock`;
    const databasePath = `${directory}/loom.sqlite`;
    const request = { ...owner, cellId: CellId.make("cell-unfinished"), source: "sideEffect()" };
    const entry = CellLedgerEntry.make({
      ...request,
      state: CellLedgerState.cases.Accepted.make({}),
    });
    yield* Effect.gen(function* () {
      const ledger = yield* CellLedger;
      yield* ledger.claim(entry);
      yield* ledger.evaluating(entry);
    }).pipe(
      Effect.provide(
        layerSqliteCellLedger.pipe(Layer.provide(layerLoomSqlite({ filename: databasePath }))),
      ),
      Effect.scoped,
    );
    const capabilities = testCapabilities({
      supports: () => false,
      execute: () => Effect.die("No Workflow capability is used by this test."),
      compensate: () => Effect.void,
    });

    const daemon = yield* runLoomDaemon(
      { workspaceRoot, socketPath, databasePath },
      capabilities,
    ).pipe(Effect.forkScoped);
    yield* withClient(workspaceRoot, socketPath, (client) => client.handshake);
    const failure = yield* withClient(workspaceRoot, socketPath, (client) =>
      client.evaluateCell(request).pipe(Effect.flip),
    );

    expect(failure).toHaveProperty("reason", "DaemonRestart");
    yield* Fiber.interrupt(daemon);
  }),
);
