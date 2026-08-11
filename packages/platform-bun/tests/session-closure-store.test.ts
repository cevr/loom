import { BunServices } from "@effect/platform-bun";
import { SessionId } from "@cvr/loom-domain";
import { SessionClosureStore } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer } from "effect";
import { TestClock } from "effect/testing";
import { layerLoomSqlite, layerSqliteSessionClosureStore } from "../src/index.js";

const scoped = it.scoped.layer(BunServices.layer);
const sessionId = SessionId.make("session-closure");

const closureLayer = (filename: string) => {
  const database = layerLoomSqlite({ filename });
  const closures = layerSqliteSessionClosureStore.pipe(Layer.provide(database));
  return Layer.merge(database, closures);
};

scoped("persists one fixed Session Closure Lease across restart", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-session-closure-" });
    const layer = closureLayer(`${directory}/loom.sqlite`);

    yield* Effect.gen(function* () {
      const closures = yield* SessionClosureStore;
      yield* closures.close(sessionId, "5 minutes");
      yield* TestClock.adjust("4 minutes");
      yield* closures.close(sessionId, "5 minutes");
    }).pipe(Effect.provide(layer), Effect.scoped);

    yield* Effect.gen(function* () {
      const closures = yield* SessionClosureStore;
      expect(yield* closures.list).toEqual([sessionId]);
      expect(yield* closures.contains(sessionId)).toBe(true);
      yield* TestClock.adjust("1 minute");
      expect(yield* closures.list).toEqual([]);
      expect(yield* closures.contains(sessionId)).toBe(false);
      expect(yield* closures.prune).toBe(1);
      expect(yield* closures.prune).toBe(0);
    }).pipe(Effect.provide(layer), Effect.scoped);
  }),
);

scoped("prunes high-cardinality expired Session closures", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-session-prune-" });

    yield* Effect.gen(function* () {
      const closures = yield* SessionClosureStore;
      yield* Effect.forEach(
        Array.from({ length: 1_000 }, (_, index) => SessionId.make(`session-${index}`)),
        (id) => closures.close(id, "5 minutes"),
        { concurrency: "unbounded", discard: true },
      );
      yield* TestClock.adjust("5 minutes");
      expect(yield* closures.prune).toBe(1_000);
    }).pipe(Effect.provide(closureLayer(`${directory}/loom.sqlite`)), Effect.scoped);
  }),
);
