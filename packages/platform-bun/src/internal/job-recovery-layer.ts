import { JobReconciler, layerJobReconciler } from "@cvr/loom-runtime";
import { Layer } from "effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import { SqlClient } from "effect/unstable/sql";
import { layerBunProcessInspector } from "./bun-process-inspector.js";
import { layerSqliteJobProcessStore } from "./sqlite-job-process-store.js";
import { layerSqliteWorkflowJobStore } from "./sqlite-workflow-job-store.js";

export const layerJobRecovery: Layer.Layer<
  JobReconciler,
  never,
  ChildProcessSpawner.ChildProcessSpawner | SqlClient.SqlClient
> = layerJobReconciler.pipe(
  Layer.provide(
    Layer.mergeAll(
      layerBunProcessInspector,
      layerSqliteJobProcessStore,
      layerSqliteWorkflowJobStore,
    ),
  ),
);
