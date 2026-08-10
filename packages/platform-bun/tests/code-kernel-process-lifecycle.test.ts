import { BunServices } from "@effect/platform-bun";
import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { CodeKernelFactory, CodeKernelProcessStore } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer } from "effect";
import {
  layerBunProcessInspector,
  layerCodeKernelFactory,
  layerLoomSqlite,
  layerSqliteCodeKernelProcessStore,
} from "../src/index.js";

const workerEntry = new URL("../../../apps/code-kernel/src/main.ts", import.meta.url).pathname;
const owner = {
  sessionId: SessionId.make("session-1"),
  agentId: AgentId.make("agent-1"),
};

it.scopedLive.layer(BunServices.layer)(
  "releases its process identity when the owner scope closes",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-owner-scope-" });
      const database = layerLoomSqlite({ filename: `${directory}/loom.sqlite` });
      const processStore = layerSqliteCodeKernelProcessStore.pipe(Layer.provide(database));
      const live = Layer.merge(
        layerCodeKernelFactory({ entryPath: workerEntry }).pipe(
          Layer.provide([layerBunProcessInspector, processStore]),
        ),
        processStore,
      );

      yield* Effect.gen(function* () {
        const factory = yield* CodeKernelFactory;
        const store = yield* CodeKernelProcessStore;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const kernel = yield* factory.spawn(owner);
            yield* kernel.evaluate({ cellId: CellId.make("cell-owned"), source: "42" });
            expect(yield* store.list).toHaveLength(1);
          }),
        );
        expect(yield* store.list).toEqual([]);
      }).pipe(Effect.provide(live));
    }),
);
