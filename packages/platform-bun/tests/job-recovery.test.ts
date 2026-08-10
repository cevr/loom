import { BunServices } from "@effect/platform-bun";
import {
  JobAddress,
  JobId,
  JobRequest,
  JobSubmission,
  ProcessIdentity,
  SessionId,
  WorkspaceRoot,
  type JobRecord,
} from "@cvr/loom-domain";
import { JobRuntime, JobStore, layerActorStateHub } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer, Option, Path, Schedule } from "effect";
import {
  layerBunJobRuntime,
  layerBunProcessController,
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteJobStore,
  makeBunProcessController,
} from "../src/index.js";

const sessionId = SessionId.make("recovery-session");
const request = (jobId: string, command: string) =>
  JobRequest.make({ jobId: JobId.make(jobId), sessionId, command, attached: true });

const storeLayer = (filename: string) =>
  layerSqliteJobStore.pipe(Layer.provide(layerLoomSqlite({ filename })));

const runtimeLayer = (filename: string, workspaceRoot: WorkspaceRoot) =>
  layerBunJobRuntime({ workspaceRoot, terminationGrace: "50 millis" }).pipe(
    Layer.provide([
      layerActorStateHub,
      layerBunProcessController,
      layerBunProcessInspector,
      storeLayer(filename),
    ]),
  );

const submission = (workspaceRoot: WorkspaceRoot, jobRequest: JobRequest) => {
  const directory = `${workspaceRoot}/.loom/jobs/${encodeURIComponent(jobRequest.jobId)}`;
  return JobSubmission.make({
    ...jobRequest,
    stdoutPath: `${directory}/stdout.log`,
    stderrPath: `${directory}/stderr.log`,
    resultPath: `${directory}/result`,
  });
};

const inspectAfterReconcile = (
  filename: string,
  workspaceRoot: WorkspaceRoot,
  address: JobAddress,
) =>
  Effect.gen(function* () {
    const runtime = yield* JobRuntime;
    yield* runtime.reconcile;
    return yield* runtime.inspect(address);
  }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);

const waitForTerminal = (runtime: JobRuntime["Service"], address: JobAddress) =>
  runtime.inspect(address).pipe(
    Effect.flatMap(
      Option.match({ onNone: () => Effect.die("Missing Job."), onSome: Effect.succeed }),
    ),
    Effect.repeat({
      until: (job) =>
        job.status === "Succeeded" ||
        job.status === "Failed" ||
        job.status === "Cancelled" ||
        job.status === "Lost",
      schedule: Schedule.spaced("10 millis"),
    }),
    Effect.timeout("5 seconds"),
  );

const seedRunning = (filename: string, job: JobSubmission) =>
  Effect.gen(function* () {
    const jobs = yield* JobStore;
    yield* jobs.create(job);
    yield* jobs.begin(job.jobId);
    yield* jobs.activate(
      job.jobId,
      ProcessIdentity.make({
        pid: 2_147_483_647,
        processGroupId: 2_147_483_647,
        processStartId: "missing-process",
      }),
    );
  }).pipe(Effect.provide(storeLayer(filename)));

const status = (job: Option.Option<JobRecord>) => Option.map(job, (record) => record.status);

it.scopedLive.layer(BunServices.layer)("fails a launch that did not commit before restart", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-starting-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-starting", "exit 0");

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.create(submission(workspaceRoot, jobRequest));
      yield* jobs.begin(jobRequest.jobId);
    }).pipe(Effect.provide(storeLayer(filename)));

    const reconciled = yield* inspectAfterReconcile(
      filename,
      workspaceRoot,
      JobAddress.make(jobRequest),
    );
    expect(status(reconciled)).toEqual(Option.some("Failed"));
  }),
);

it.scopedLive.layer(BunServices.layer)("restarts an accepted Job", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-accepted-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-accepted", "printf 'restarted\\n'");
    const address = JobAddress.make(jobRequest);

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.create(submission(workspaceRoot, jobRequest));
    }).pipe(Effect.provide(storeLayer(filename)));

    const completed = yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.reconcile;
      return yield* waitForTerminal(runtime, address);
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);
    expect(completed.status).toBe("Succeeded");
    expect(yield* fs.readFileString(completed.stdoutPath)).toBe("restarted\n");
  }),
);

it.scopedLive.layer(BunServices.layer)("recovers a durable result after the process exits", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-result-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-result", "exit 0");
    const job = submission(workspaceRoot, jobRequest);
    const path = yield* Path.Path;

    yield* fs.makeDirectory(path.dirname(job.resultPath), { recursive: true });
    yield* fs.writeFileString(job.resultPath, "0\n");
    yield* seedRunning(filename, job);

    const reconciled = yield* inspectAfterReconcile(
      filename,
      workspaceRoot,
      JobAddress.make(jobRequest),
    );
    expect(status(reconciled)).toEqual(Option.some("Succeeded"));
  }),
);

it.scopedLive.layer(BunServices.layer)("marks a missing process without a result as Lost", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-lost-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-lost", "exit 0");
    const job = submission(workspaceRoot, jobRequest);

    yield* seedRunning(filename, job);
    const reconciled = yield* inspectAfterReconcile(
      filename,
      workspaceRoot,
      JobAddress.make(jobRequest),
    );
    expect(status(reconciled)).toEqual(Option.some("Lost"));
  }),
);

it.scopedLive.layer(BunServices.layer)("marks an identity mismatch as Lost", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-mismatch-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-mismatch", "exit 0");
    const job = submission(workspaceRoot, jobRequest);

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.create(job);
      yield* jobs.begin(job.jobId);
      yield* jobs.activate(
        job.jobId,
        ProcessIdentity.make({ pid: 1, processGroupId: 1, processStartId: "wrong-start" }),
      );
    }).pipe(Effect.provide(storeLayer(filename)));

    const reconciled = yield* inspectAfterReconcile(
      filename,
      workspaceRoot,
      JobAddress.make(jobRequest),
    );
    expect(status(reconciled)).toEqual(Option.some("Lost"));
  }),
);

it.scopedLive.layer(BunServices.layer)("isolates a failed Job reconciliation pass", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-isolation-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const invalidRequest = request("a-invalid-result", "exit 0");
    const acceptedRequest = request("b-accepted", "exit 0");
    const invalid = submission(workspaceRoot, invalidRequest);
    const path = yield* Path.Path;

    yield* fs.makeDirectory(path.dirname(invalid.resultPath), { recursive: true });
    yield* fs.writeFileString(invalid.resultPath, "not-an-exit-code\n");
    yield* seedRunning(filename, invalid);
    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.create(submission(workspaceRoot, acceptedRequest));
    }).pipe(Effect.provide(storeLayer(filename)));

    yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.reconcile;
      expect((yield* waitForTerminal(runtime, JobAddress.make(acceptedRequest))).status).toBe(
        "Succeeded",
      );
      expect(status(yield* runtime.inspect(JobAddress.make(invalidRequest)))).toEqual(
        Option.some("Running"),
      );
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);
  }),
);

it.scopedLive.layer(BunServices.layer)("resumes cancellation after restart", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-stopping-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const filename = `${directory}/loom.sqlite`;
    const jobRequest = request("job-stopping", "trap '' TERM; while :; do sleep 1; done");
    const address = JobAddress.make(jobRequest);

    const running = yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.start(jobRequest);
      return yield* runtime.inspect(address).pipe(
        Effect.flatMap(
          Option.match({ onNone: () => Effect.die("Missing Job."), onSome: Effect.succeed }),
        ),
        Effect.repeat({
          until: (job) => job.status === "Running",
          schedule: Schedule.spaced("10 millis"),
        }),
        Effect.timeout("5 seconds"),
      );
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.requestStop(address);
    }).pipe(Effect.provide(storeLayer(filename)));

    const reconciled = yield* Effect.gen(function* () {
      const runtime = yield* JobRuntime;
      yield* runtime.reconcile;
      return Option.some(yield* waitForTerminal(runtime, address));
    }).pipe(Effect.provide(runtimeLayer(filename, workspaceRoot)), Effect.scoped);

    expect(status(reconciled)).toEqual(Option.some("Cancelled"));
    expect(yield* makeBunProcessController.isGroupAlive(Option.getOrThrow(running.identity))).toBe(
      false,
    );
  }),
);
