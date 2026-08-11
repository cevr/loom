import { BunServices } from "@effect/platform-bun";
import { JobAddress, JobFailure, JobOutcome, WorkspaceRoot } from "@cvr/loom-domain";
import {
  JobRuntime,
  JobStore,
  ProcessInspectionError,
  ProcessInspector,
  layerActorStateHub,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer, Option } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { layerBunJobRuntime, layerBunProcessController, layerLoomSqlite } from "../src/index.js";
import {
  inspectAfterReconcile,
  request,
  runtimeLayer,
  seedRunning,
  status,
  storeLayer,
  submission,
  waitForTerminal,
} from "./job-recovery-test-support.js";

it.scopedLive.layer(BunServices.layer)("cancels an uncommitted stopping Job after restart", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-stopping-startup-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-stopping-startup", "exit 0");

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.create(submission(workspaceRoot, jobRequest));
      yield* jobs.begin(jobRequest.jobId);
      yield* jobs.requestStop(JobAddress.make(jobRequest));
    }).pipe(Effect.provide(storeLayer(filename)));

    const reconciled = yield* inspectAfterReconcile(
      filename,
      workspaceRoot,
      JobAddress.make(jobRequest),
    );
    expect(status(reconciled)).toEqual(Option.some("Cancelled"));
  }),
);

it.scopedLive.layer(BunServices.layer)("retries a failed launch with the same Job ID", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-launch-retry-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-launch-retry", "printf 'retried\\n'");
    const address = JobAddress.make(jobRequest);

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.create(submission(workspaceRoot, jobRequest));
      yield* jobs.begin(jobRequest.jobId);
      yield* jobs.complete(
        jobRequest.jobId,
        JobOutcome.cases.Failed.make({
          failure: JobFailure.cases.Launch.make({ detail: "First launch failed." }),
        }),
      );
    }).pipe(Effect.provide(storeLayer(filename)));

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      expect((yield* runtime.start(jobRequest)).status).toBe("Starting");
      expect((yield* waitForTerminal(runtime, address)).status).toBe("Succeeded");
      expect(yield* fs.readFileString(`${directory}/.loom/jobs/job-launch-retry/stdout.log`)).toBe(
        "retried\n",
      );
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);
  }),
);

it.scopedLive.layer(BunServices.layer)("keeps the launch gate closed until identity commits", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-launch-gate-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const marker = `${directory}/command-started`;
    const jobRequest = request("job-launch-gate", `printf started > '${marker}'`);

    yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`
        CREATE TRIGGER deny_job_activation
        BEFORE UPDATE OF status ON jobs
        WHEN NEW.status = 'Running'
        BEGIN
          SELECT RAISE(ABORT, 'activation denied');
        END
      `;
    }).pipe(Effect.provide(layerLoomSqlite({ filename })));

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.start(jobRequest);
      const failed = yield* waitForTerminal(runtime, JobAddress.make(jobRequest));
      expect(failed.status).toBe("Failed");
      expect(failed.status === "Failed" && JobFailure.guards.Runtime(failed.failure)).toBe(true);
      expect((yield* runtime.start(jobRequest)).status).toBe("Failed");
      expect(yield* fs.exists(marker)).toBe(false);
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);
  }),
);

it.scopedLive.layer(BunServices.layer)("cancels a stopping Job whose process is absent", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-stopping-missing-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-stopping-missing", "exit 0");
    const address = JobAddress.make(jobRequest);

    yield* seedRunning(filename, submission(workspaceRoot, jobRequest));
    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.requestStop(address);
    }).pipe(Effect.provide(storeLayer(filename)));

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.reconcile;
      expect((yield* waitForTerminal(runtime, address)).status).toBe("Cancelled");
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);
  }),
);

it.scopedLive.layer(BunServices.layer)("leaves a Job active when process inspection fails", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-inspection-failure-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-inspection-failure", "exit 0");
    const address = JobAddress.make(jobRequest);

    yield* seedRunning(filename, submission(workspaceRoot, jobRequest));
    const failingInspector = Layer.succeed(
      ProcessInspector,
      ProcessInspector.of({
        inspect: (pid) =>
          Effect.fail(new ProcessInspectionError({ pid, cause: "Test inspection failure." })),
      }),
    );
    const runtime = layerBunJobRuntime({
      workspaceRoot,
      terminationGrace: "50 millis",
    }).pipe(
      Layer.provide([
        layerActorStateHub,
        layerBunProcessController,
        failingInspector,
        storeLayer(filename),
      ]),
    );

    const reconciled = yield* Effect.gen(function* () {
      const jobs = yield* JobRuntime;
      yield* jobs.reconcile;
      return yield* jobs.inspect(address);
    }).pipe(Effect.provide(runtime), Effect.scoped);
    expect(status(reconciled)).toEqual(Option.some("Running"));
  }),
);
