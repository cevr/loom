import { layerJobRecovery } from "@cvr/loom-platform-bun";
import { JobReconciler } from "@cvr/loom-runtime";
import { Config, Effect, FileSystem, Path } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";

export const startupMessage = Effect.succeed("Loom daemon is ready");

const databasePath = Config.nonEmptyString("LOOM_DB_PATH").pipe(
  Config.withDefault(".loom/loom.sqlite"),
);

const reconcileJobs = Effect.fn("LoomDaemon.reconcileJobs")(function* (filename: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fs.makeDirectory(path.dirname(filename), { recursive: true });
  const results = yield* Effect.gen(function* () {
    const reconciler = yield* JobReconciler;
    return yield* reconciler.reconcile;
  }).pipe(Effect.provide(layerJobRecovery({ filename })));
  yield* Effect.logInfo("Job restart reconciliation completed.", results);
});

export const program: Effect.Effect<
  void,
  unknown,
  FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner
> = Effect.gen(function* () {
  yield* reconcileJobs(yield* databasePath);
  yield* Effect.logInfo(yield* startupMessage);
});
