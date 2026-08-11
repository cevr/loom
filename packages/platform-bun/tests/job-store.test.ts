import { BunServices } from "@effect/platform-bun";
import {
  JobAddress,
  JobFailure,
  JobId,
  JobOutcome,
  JobRecord,
  JobSubmission,
  ProcessIdentity,
  SessionId,
} from "@cvr/loom-domain";
import { JobStore, JobStoreError } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, Exit, FileSystem, Layer, Option } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { layerLoomSqlite, layerSqliteJobStore } from "../src/index.js";

const sessionId = SessionId.make("session-1");
const jobId = JobId.make("job-1");
const address = JobAddress.make({ sessionId, jobId });
const submission = JobSubmission.make({
  jobId,
  sessionId,
  command: "sleep 30",
  attached: true,
  stdoutPath: "/tmp/job-1/stdout.log",
  stderrPath: "/tmp/job-1/stderr.log",
  resultPath: "/tmp/job-1/result",
});
const accepted = JobRecord.make({
  ...submission,
  status: "Accepted",
});
const identity = ProcessIdentity.make({
  pid: 42001,
  processGroupId: 42001,
  processStartId: "Sun Aug  9 10:00:00 2026",
});
const invalidRunningJobId = JobId.make("job-2");
const invalidSucceededJobId = JobId.make("job-3");
const invalidFailedJobId = JobId.make("job-4");
const invalidLostJobId = JobId.make("job-5");
const layerJobStore = (filename: string) => {
  const sqlite = layerLoomSqlite({ filename });
  return layerSqliteJobStore.pipe(Layer.provideMerge(sqlite));
};

const insertPartialIdentity = (sql: SqlClient.SqlClient) => sql`
  INSERT INTO jobs (
    job_id, session_id, command, attached, status,
    stdout_path, stderr_path, result_path, pid
  ) VALUES (
    ${jobId}, ${sessionId}, 'sleep 30', 1, 'Starting',
    '/tmp/job-1/stdout.log', '/tmp/job-1/stderr.log', '/tmp/job-1/result', 42001
  )
`;

const insertRunningWithoutIdentity = (sql: SqlClient.SqlClient, runningJobId: JobId) => sql`
  INSERT INTO jobs (
    job_id, session_id, command, attached, status,
    stdout_path, stderr_path, result_path
  ) VALUES (
    ${runningJobId}, ${sessionId}, 'sleep 30', 1, 'Running',
    '/tmp/job-2/stdout.log', '/tmp/job-2/stderr.log', '/tmp/job-2/result'
  )
`;

const insertSuccessWithoutExitCode = (sql: SqlClient.SqlClient, succeededJobId: JobId) => sql`
  INSERT INTO jobs (
    job_id, session_id, command, attached, status,
    stdout_path, stderr_path, result_path,
    pid, process_group_id, process_start_id
  ) VALUES (
    ${succeededJobId}, ${sessionId}, 'sleep 30', 1, 'Succeeded',
    '/tmp/job-3/stdout.log', '/tmp/job-3/stderr.log', '/tmp/job-3/result',
    42003, 42003, 'Sun Aug  9 10:00:00 2026'
  )
`;

const insertFailedWithSuccessCode = (sql: SqlClient.SqlClient, failedJobId: JobId) => sql`
  INSERT INTO jobs (
    job_id, session_id, command, attached, status,
    stdout_path, stderr_path, result_path, failure_kind, exit_code
  ) VALUES (
    ${failedJobId}, ${sessionId}, 'exit 0', 1, 'Failed',
    '/tmp/job-4/stdout.log', '/tmp/job-4/stderr.log', '/tmp/job-4/result', 'Exit', 0
  )
`;

const insertLostWithIdentity = (sql: SqlClient.SqlClient, lostJobId: JobId) => sql`
  INSERT INTO jobs (
    job_id, session_id, command, attached, status,
    stdout_path, stderr_path, result_path, pid, process_group_id, process_start_id
  ) VALUES (
    ${lostJobId}, ${sessionId}, 'sleep 30', 1, 'Lost',
    '/tmp/job-5/stdout.log', '/tmp/job-5/stderr.log', '/tmp/job-5/result',
    42005, 42005, 'Sun Aug  9 10:00:00 2026'
  )
`;

const assertForeignWritesFail = Effect.fn(function* (sql: SqlClient.SqlClient) {
  expect(Exit.isFailure(yield* Effect.exit(insertPartialIdentity(sql)))).toBe(true);
  expect(
    Exit.isFailure(yield* Effect.exit(insertRunningWithoutIdentity(sql, invalidRunningJobId))),
  ).toBe(true);
  expect(
    Exit.isFailure(yield* Effect.exit(insertSuccessWithoutExitCode(sql, invalidSucceededJobId))),
  ).toBe(true);
  expect(
    Exit.isFailure(yield* Effect.exit(insertFailedWithSuccessCode(sql, invalidFailedJobId))),
  ).toBe(true);
  expect(Exit.isFailure(yield* Effect.exit(insertLostWithIdentity(sql, invalidLostJobId)))).toBe(
    true,
  );
});

const assertForeignRowsFailToDecode = Effect.fn(function* (
  sql: SqlClient.SqlClient,
  jobs: JobStore["Service"],
) {
  yield* sql`PRAGMA ignore_check_constraints = ON`;
  yield* insertPartialIdentity(sql);
  yield* insertRunningWithoutIdentity(sql, invalidRunningJobId);
  yield* insertSuccessWithoutExitCode(sql, invalidSucceededJobId);
  yield* insertFailedWithSuccessCode(sql, invalidFailedJobId);
  yield* insertLostWithIdentity(sql, invalidLostJobId);
  for (const foreignJobId of [
    jobId,
    invalidRunningJobId,
    invalidSucceededJobId,
    invalidFailedJobId,
    invalidLostJobId,
  ]) {
    const foreignAddress = JobAddress.make({ sessionId, jobId: foreignJobId });
    expect(Exit.isFailure(yield* Effect.exit(jobs.get(foreignAddress)))).toBe(true);
  }
});

it.scopedLive.layer(BunServices.layer)("owns the durable Job lifecycle", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-store-" });

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      expect(Option.map(yield* jobs.create(submission), (job) => job.status)).toEqual(
        Option.some("Accepted"),
      );
      expect(yield* jobs.create(submission)).toEqual(Option.none());
      expect(yield* jobs.get(address)).toEqual(Option.some(accepted));
      expect(Option.map(yield* jobs.begin(jobId), (job) => job.status)).toEqual(
        Option.some("Starting"),
      );
      expect(yield* jobs.begin(jobId)).toEqual(Option.none());

      const running = yield* jobs.activate(jobId, identity);
      expect(Option.map(running, (job) => job.status)).toEqual(Option.some("Running"));
      expect((yield* jobs.listRecoverable).map((job) => job.status)).toEqual(["Running"]);
      expect(yield* jobs.listAttachedActive(sessionId)).toHaveLength(1);

      const detached = yield* jobs.detach(address);
      expect(Option.map(detached, (job) => job.attached)).toEqual(Option.some(false));
      expect(yield* jobs.listAttachedActive(sessionId)).toEqual([]);

      const stopping = yield* jobs.requestStop(address);
      expect(Option.map(stopping, (job) => job.status)).toEqual(Option.some("Stopping"));
      const completed = yield* jobs.complete(jobId, JobOutcome.cases.Cancelled.make({}));
      expect(Option.map(completed, (job) => job.status)).toEqual(Option.some("Cancelled"));
      expect(yield* jobs.complete(jobId, JobOutcome.cases.Succeeded.make({ exitCode: 0 }))).toEqual(
        Option.none(),
      );
      expect(yield* jobs.detach(address)).toEqual(Option.none());
      expect(Option.map(yield* jobs.get(address), (job) => job.status)).toEqual(
        Option.some("Cancelled"),
      );
    }).pipe(Effect.provide(layerJobStore(`${directory}/loom.sqlite`)));
  }),
);

it.scopedLive.layer(BunServices.layer)("cancels an accepted Job without launching it", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-cancel-" });

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.create(submission);
      expect((yield* jobs.listUncommitted).map((job) => job.status)).toEqual(["Accepted"]);
      const cancelled = yield* jobs.requestStop(address);
      expect(Option.map(cancelled, (job) => job.status)).toEqual(Option.some("Cancelled"));
      expect(yield* jobs.begin(jobId)).toEqual(Option.none());
      expect(yield* jobs.listUncommitted).toEqual([]);
    }).pipe(Effect.provide(layerJobStore(`${directory}/loom.sqlite`)));
  }),
);

it.scopedLive.layer(BunServices.layer)(
  "records identity after cancellation wins the launch race",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-stop-race-" });

      yield* Effect.gen(function* () {
        const jobs = yield* JobStore;
        yield* jobs.create(submission);
        yield* jobs.begin(jobId);
        yield* jobs.requestStop(address);
        const stopping = yield* jobs.activate(jobId, identity);
        expect(Option.map(stopping, (job) => job.status)).toEqual(Option.some("Stopping"));
        expect(
          stopping.pipe(
            Option.filter(JobRecord.guards.Stopping),
            Option.flatMap((job) => job.identity),
          ),
        ).toEqual(Option.some(identity));
        expect(yield* jobs.activate(jobId, { ...identity, pid: 42002 })).toEqual(Option.none());
      }).pipe(Effect.provide(layerJobStore(`${directory}/loom.sqlite`)));
    }),
);

it.scopedLive.layer(BunServices.layer)("persists successful and failed Job outcomes", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-outcome-" });

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      yield* jobs.create(submission);
      yield* jobs.begin(jobId);
      yield* jobs.activate(jobId, identity);
      const completed = yield* jobs.complete(
        jobId,
        JobOutcome.cases.Succeeded.make({ exitCode: 0 }),
      );
      expect(Option.map(completed, (job) => job.status)).toEqual(Option.some("Succeeded"));
      const succeeded = yield* jobs.get(address);
      expect(
        succeeded.pipe(
          Option.filter(JobRecord.guards.Succeeded),
          Option.map((job) => [job.status, job.exitCode]),
        ),
      ).toEqual(Option.some(["Succeeded", 0]));

      const failedJobId = JobId.make("job-2");
      const failedAddress = JobAddress.make({ sessionId, jobId: failedJobId });
      yield* jobs.create(JobSubmission.make({ ...submission, jobId: failedJobId }));
      yield* jobs.begin(failedJobId);
      const failed = yield* jobs.complete(
        failedJobId,
        JobOutcome.cases.Failed.make({
          failure: JobFailure.cases.Launch.make({ detail: "launch failed" }),
        }),
      );
      expect(Option.map(failed, (job) => job.status)).toEqual(Option.some("Failed"));
      expect(
        (yield* jobs.get(failedAddress)).pipe(
          Option.filter(JobRecord.guards.Failed),
          Option.map((job) => job.failure),
          Option.filter(JobFailure.guards.Launch),
          Option.map((failure) => failure.detail),
        ),
      ).toEqual(Option.some("launch failed"));
      expect(Option.map(yield* jobs.begin(failedJobId), (job) => job.status)).toEqual(
        Option.some("Starting"),
      );
    }).pipe(Effect.provide(layerJobStore(`${directory}/loom.sqlite`)));
  }),
);

it.scopedLive.layer(BunServices.layer)("reports a typed store failure", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-error-" });

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`DROP TABLE jobs`;
      const error = yield* Effect.flip(jobs.create(submission));
      expect(error).toBeInstanceOf(JobStoreError);
      expect(error.operation).toBe("create");
    }).pipe(Effect.provide(layerJobStore(`${directory}/loom.sqlite`)));
  }),
);

it.scopedLive.layer(BunServices.layer)("rejects invalid foreign Job writes", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-identity-" });

    yield* Effect.gen(function* () {
      const jobs = yield* JobStore;
      const sql = yield* SqlClient.SqlClient;
      yield* assertForeignWritesFail(sql);
      yield* assertForeignRowsFailToDecode(sql, jobs);
    }).pipe(Effect.provide(layerJobStore(`${directory}/loom.sqlite`)));
  }),
);
