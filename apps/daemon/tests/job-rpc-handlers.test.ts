import { BunServices } from "@effect/platform-bun";
import { JobId, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import {
  layerBunJobRuntime,
  layerBunProcessController,
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteJobStore,
} from "@cvr/loom-platform-bun";
import { JobRuntime, layerActorStateHub } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer } from "effect";
import { makeJobRpcHandlers } from "../src/job-rpc-handlers.js";

const sessionId = SessionId.make("job-rpc-session");

const runtimeLayer = (filename: string, workspaceRoot: WorkspaceRoot) => {
  const store = layerSqliteJobStore.pipe(Layer.provide(layerLoomSqlite({ filename })));
  return layerBunJobRuntime({ workspaceRoot, terminationGrace: "50 millis" }).pipe(
    Layer.provide([layerActorStateHub, layerBunProcessController, layerBunProcessInspector, store]),
  );
};

it.scopedLive.layer(BunServices.layer)("controls a durable Job through the RPC handlers", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-rpc-" });
    const workspaceRoot = WorkspaceRoot.make(directory);

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      const handlers = makeJobRpcHandlers(runtime);
      const jobId = JobId.make("job-rpc");
      const started = yield* handlers["Job.Start"]({
        sessionId,
        jobId,
        command: "sleep 0.05; printf 'through-rpc'",
        attached: true,
        foregroundLeaseMillis: 0,
      });
      const duplicate = yield* handlers["Job.Start"]({
        sessionId,
        jobId,
        command: "sleep 0.05; printf 'through-rpc'",
        attached: true,
        foregroundLeaseMillis: 0,
      });
      const detached = yield* handlers["Job.Detach"]({ sessionId, jobId });
      const completed = yield* handlers["Job.Await"]({
        sessionId,
        jobId,
        foregroundLeaseMillis: 5_000,
      });
      const output = yield* handlers["Job.Output"]({
        sessionId,
        jobId,
        stream: "stdout",
        sequence: 0,
        maximumBytes: 128,
      });
      const inspected = yield* handlers["Job.Inspect"]({ sessionId, jobId });

      expect(started.jobId).toBe(jobId);
      expect(duplicate.jobId).toBe(jobId);
      expect(completed.status).toBe("Succeeded");
      expect(new TextDecoder().decode(output.data)).toBe("through-rpc");
      expect(output.complete).toBe(true);
      expect(detached.attached).toBe(false);
      expect(inspected.status).toBe("Succeeded");
    }).pipe(Effect.provide(runtimeLayer(`${directory}/loom.sqlite`, workspaceRoot)), Effect.scoped);
  }),
);

it.scopedLive.layer(BunServices.layer)("reads Job output through RPC after restart", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-rpc-restart-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobId = JobId.make("job-rpc-restart");
    const request = {
      sessionId,
      jobId,
      command: "printf 'before-restart'; sleep 30",
      attached: false,
      foregroundLeaseMillis: 20,
    };

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* makeJobRpcHandlers(runtime)["Job.Start"](request);
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      const handlers = makeJobRpcHandlers(runtime);
      yield* runtime.reconcile;
      const output = yield* handlers["Job.Output"]({
        sessionId,
        jobId,
        stream: "stdout",
        sequence: 0,
        maximumBytes: 128,
      });
      expect(new TextDecoder().decode(output.data)).toBe("before-restart");
      yield* handlers["Job.Cancel"]({ sessionId, jobId });
      expect(
        (yield* handlers["Job.Await"]({ sessionId, jobId, foregroundLeaseMillis: 5_000 })).status,
      ).toBe("Cancelled");
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);
  }),
);

it.scopedLive.layer(BunServices.layer)("cancels a running Job through the RPC handler", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-rpc-cancel-" });
    const workspaceRoot = WorkspaceRoot.make(directory);

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      const handlers = makeJobRpcHandlers(runtime);
      const jobId = JobId.make("job-rpc-cancel");
      yield* handlers["Job.Start"]({
        sessionId,
        jobId,
        command: "sleep 30",
        attached: true,
        foregroundLeaseMillis: 20,
      });
      expect((yield* handlers["Job.Cancel"]({ sessionId, jobId })).status).toBe("Cancelled");
    }).pipe(Effect.provide(runtimeLayer(`${directory}/loom.sqlite`, workspaceRoot)), Effect.scoped);
  }),
);
