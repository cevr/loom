import { BunServices } from "@effect/platform-bun";
import {
  AgentId,
  CellId,
  CodeKernelProcessRecord,
  processIdentitiesMatch,
  SessionId,
} from "@cvr/loom-domain";
import {
  CodeKernelFactory,
  CodeKernelProcessStore,
  ProcessController,
  ProcessInspector,
  ProcessObservation,
  reconcileCodeKernelProcesses,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Array as Arr, Effect, FileSystem, Layer } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import {
  layerBunProcessController,
  layerBunProcessInspector,
  layerCodeKernelFactory,
  layerLoomSqlite,
  layerSqliteCodeKernelProcessStore,
} from "../src/index.js";

const workerEntry = new URL("../../../apps/code-kernel/src/main.ts", import.meta.url).pathname;
const owner = {
  sessionId: SessionId.make("session-kernel-recovery"),
  agentId: AgentId.make("agent-kernel-recovery"),
};
const scopedLive = it.scopedLive.layer(BunServices.layer);

const layerTestEnvironment = (filename: string) => {
  const persistence = layerSqliteCodeKernelProcessStore.pipe(
    Layer.provideMerge(layerLoomSqlite({ filename })),
  );
  const infrastructure = Layer.merge(persistence, layerBunProcessInspector);
  const factory = layerCodeKernelFactory({ entryPath: workerEntry }).pipe(
    Layer.provideMerge(infrastructure),
  );
  return Layer.merge(factory, layerBunProcessController);
};

const reconcile = Effect.gen(function* () {
  yield* reconcileCodeKernelProcesses({
    store: yield* CodeKernelProcessStore,
    inspector: yield* ProcessInspector,
    controller: yield* ProcessController,
  });
});

const foundIdentity = (pid: number) =>
  ProcessInspector.use((inspector) =>
    inspector.inspect(pid).pipe(
      Effect.flatMap(
        ProcessObservation.$match({
          Missing: () => Effect.die(`Process ${pid} is missing.`),
          Found: ({ identity }) => Effect.succeed(identity),
        }),
      ),
    ),
  );

const spawnSleeper = Effect.gen(function* () {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  return yield* spawner.spawn(
    ChildProcess.make("sleep", ["30"], {
      stdin: "ignore",
      stdout: "ignore",
      stderr: "ignore",
    }),
  );
});

scopedLive("registers a real Bun Code Kernel before use and removes it after exit", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-process-" });
    yield* Effect.gen(function* () {
      const factory = yield* CodeKernelFactory;
      const store = yield* CodeKernelProcessStore;
      const inspector = yield* ProcessInspector;
      const kernel = yield* factory.spawn(owner);

      const evaluation = yield* kernel.evaluate({
        cellId: CellId.make("cell-kernel-process"),
        source: "6 * 7",
      });
      const records = yield* store.list;
      const record = yield* Effect.fromOption(Arr.head(records));
      const observation = yield* inspector.inspect(record.pid);

      expect(evaluation.display).toBe("42");
      expect(record).toMatchObject(owner);
      expect(
        ProcessObservation.$match(observation, {
          Missing: () => false,
          Found: ({ identity }) => processIdentitiesMatch(record, identity),
        }),
      ).toBe(true);

      yield* kernel.close;
      expect(yield* store.list).toEqual([]);
    }).pipe(Effect.provide(layerTestEnvironment(`${directory}/loom.sqlite`)));
  }),
);

scopedLive("terminates an exact stored process identity", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-orphan-" });
    yield* Effect.gen(function* () {
      const store = yield* CodeKernelProcessStore;
      const controller = yield* ProcessController;
      const child = yield* spawnSleeper;
      const identity = yield* foundIdentity(child.pid);
      const record = CodeKernelProcessRecord.make({ ...owner, ...identity });

      expect(yield* store.register(record)).toBe(true);
      yield* reconcile;

      expect(yield* controller.isGroupAlive(identity)).toBe(false);
      expect(yield* store.list).toEqual([]);
    }).pipe(Effect.provide(layerTestEnvironment(`${directory}/loom.sqlite`)));
  }),
);

scopedLive("does not signal a reused PID", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-pid-reuse-" });
    yield* Effect.gen(function* () {
      const store = yield* CodeKernelProcessStore;
      const controller = yield* ProcessController;
      const child = yield* spawnSleeper;
      const identity = yield* foundIdentity(child.pid);
      const stale = CodeKernelProcessRecord.make({
        ...owner,
        ...identity,
        processStartId: `${identity.processStartId} stale`,
      });

      expect(yield* store.register(stale)).toBe(true);
      yield* reconcile;

      expect(yield* controller.isGroupAlive(identity)).toBe(true);
      expect(yield* store.list).toEqual([]);
    }).pipe(Effect.provide(layerTestEnvironment(`${directory}/loom.sqlite`)));
  }),
);

scopedLive("removes only the exact registered identity", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-exact-remove-" });
    yield* CodeKernelProcessStore.use((store) =>
      Effect.gen(function* () {
        const registered = CodeKernelProcessRecord.make({
          ...owner,
          pid: 42_001,
          processGroupId: 42_001,
          processStartId: "Mon Aug 10 21:00:00 2026",
        });
        const replacement = CodeKernelProcessRecord.make({
          ...registered,
          pid: 42_002,
          processGroupId: 42_002,
          processStartId: "Mon Aug 10 21:00:01 2026",
        });

        expect(yield* store.register(registered)).toBe(true);
        expect(yield* store.register(replacement)).toBe(false);
        expect(yield* store.remove(replacement)).toBe(false);
        expect(yield* store.list).toEqual([registered]);
        expect(yield* store.remove(registered)).toBe(true);
      }),
    ).pipe(Effect.provide(layerTestEnvironment(`${directory}/loom.sqlite`)));
  }),
);
