import { BunServices } from "@effect/platform-bun";
import { JobAddress, JobId, JobRequest, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { JobRuntime, layerActorStateHub } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, Fiber, FileSystem, Layer, Option, Schedule } from "effect";
import {
  layerBunJobRuntime,
  layerBunProcessController,
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteJobStore,
} from "../src/index.js";

const waitForStatus = (runtime: JobRuntime["Service"], address: JobAddress, status: string) =>
  runtime.inspect(address).pipe(
    Effect.repeat({
      until: Option.exists((job) => job.status === status),
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.timeout("5 seconds"),
  );

it.scopedLive.layer(BunServices.layer)("survives cancellation caller interruption", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-cancel-owner-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const request = JobRequest.make({
      sessionId: SessionId.make("cancel-owner-session"),
      jobId: JobId.make("job-cancel-owner"),
      command: "trap '' TERM; while :; do sleep 1; done",
      attached: true,
    });
    const address = JobAddress.make(request);
    const store = layerSqliteJobStore.pipe(
      Layer.provide(layerLoomSqlite({ filename: `${directory}/loom.sqlite` })),
    );
    const runtimeLayer = layerBunJobRuntime({
      workspaceRoot,
      terminationGrace: "500 millis",
    }).pipe(
      Layer.provide([
        layerActorStateHub,
        layerBunProcessController,
        layerBunProcessInspector,
        store,
      ]),
    );

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.start(request);
      yield* waitForStatus(runtime, address, "Running");
      const caller = yield* runtime.cancel(address).pipe(Effect.forkScoped);
      yield* waitForStatus(runtime, address, "Stopping");
      yield* Fiber.interrupt(caller);
      const terminal = yield* waitForStatus(runtime, address, "Cancelled");
      expect(Option.map(terminal, (job) => job.status)).toEqual(Option.some("Cancelled"));
    }).pipe(Effect.provide(runtimeLayer));
  }),
);
