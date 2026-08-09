import { BunRuntime, BunServices } from "@effect/platform-bun";
import { JobReconciler } from "@cvr/loom-runtime";
import { Config, Effect, Layer } from "effect";
import { layerJobRecovery } from "../src/index.js";

const runtimeLayer = (filename: string) =>
  layerJobRecovery({ filename }).pipe(Layer.provide(BunServices.layer));

const program = Effect.gen(function* () {
  const filename = yield* Config.string("LOOM_PROBE_DB");
  const results = yield* Effect.gen(function* () {
    const reconciler = yield* JobReconciler;
    return yield* reconciler.reconcile;
  }).pipe(Effect.provide(runtimeLayer(filename)));
  yield* Effect.log("Restart reconciliation completed.", results);
});

BunRuntime.runMain(program);
