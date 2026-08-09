import { SqliteClient } from "@effect/sql-sqlite-bun";
import { JobReconciler, layerJobReconciler } from "@cvr/loom-runtime";
import { Layer } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import { layerBunProcessInspector } from "./bun-process-inspector.js";
import { layerSqliteJobProcessStore } from "./sqlite-job-process-store.js";

export interface JobRecoveryConfig {
  readonly filename: string;
}

export const layerJobRecovery = (
  config: JobRecoveryConfig,
): Layer.Layer<JobReconciler, never, ChildProcessSpawner.ChildProcessSpawner> =>
  layerJobReconciler.pipe(
    Layer.provide(Layer.merge(layerBunProcessInspector, layerSqliteJobProcessStore)),
    Layer.provide(SqliteClient.layer({ filename: config.filename })),
  );
