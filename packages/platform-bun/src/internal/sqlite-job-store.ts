import {
  JobActiveStatus,
  JobAddress,
  JobId,
  JobOutcome,
  JobRecord,
  JobStartedStatus,
  JobSubmission,
  ProcessIdentity,
  SessionId,
  type JobTerminalStatus,
} from "@cvr/loom-domain";
import { JobStore, JobStoreError, type JobStoreShape } from "@cvr/loom-runtime";
import { Effect, Layer, Option, Schema, SchemaTransformation } from "effect";
import type { NonEmptyReadonlyArray } from "effect/Array";
import { SqlClient, SqlSchema } from "effect/unstable/sql";
import type { SqlError } from "effect/unstable/sql/SqlError";

const FlatJob = Schema.Struct({
  jobId: JobId,
  sessionId: SessionId,
  command: JobRecord.fields.command,
  attached: Schema.BooleanFromBit,
  status: JobRecord.fields.status,
  stdoutPath: JobRecord.fields.stdoutPath,
  stderrPath: JobRecord.fields.stderrPath,
  resultPath: JobRecord.fields.resultPath,
  pid: Schema.OptionFromNullOr(ProcessIdentity.fields.pid),
  processGroupId: Schema.OptionFromNullOr(ProcessIdentity.fields.processGroupId),
  processStartId: Schema.OptionFromNullOr(ProcessIdentity.fields.processStartId),
  exitCode: JobRecord.fields.exitCode,
  detail: JobRecord.fields.detail,
}).pipe(
  Schema.encodeKeys({
    jobId: "job_id",
    sessionId: "session_id",
    stdoutPath: "stdout_path",
    stderrPath: "stderr_path",
    resultPath: "result_path",
    processGroupId: "process_group_id",
    processStartId: "process_start_id",
    exitCode: "exit_code",
  }),
  Schema.check(
    Schema.makeFilter(
      (row) =>
        Option.isSome(row.pid) === Option.isSome(row.processGroupId) &&
        Option.isSome(row.pid) === Option.isSome(row.processStartId) &&
        (row.status !== "Running" || Option.isSome(row.pid)) &&
        (row.status !== "Succeeded" || Option.getOrNull(row.exitCode) === 0),
      { expected: "a valid Job state" },
    ),
  ),
);

const JobRow = FlatJob.pipe(
  Schema.decodeTo(
    Schema.toType(JobRecord),
    SchemaTransformation.transform({
      decode: (row) =>
        JobRecord.make({
          ...row,
          identity: Option.all({
            pid: row.pid,
            processGroupId: row.processGroupId,
            processStartId: row.processStartId,
          }),
        }),
      encode: (job) => ({
        ...job,
        pid: Option.map(job.identity, (identity) => identity.pid),
        processGroupId: Option.map(job.identity, (identity) => identity.processGroupId),
        processStartId: Option.map(job.identity, (identity) => identity.processStartId),
      }),
    }),
  ),
);

const mapStoreError = (operation: string) =>
  Effect.mapError((cause: SqlError) => new JobStoreError({ operation, cause }));

const findOne = <Request extends Schema.Constraint>(
  operation: string,
  Request: Request,
  execute: (request: Request["Encoded"]) => Effect.Effect<ReadonlyArray<unknown>, SqlError>,
) => {
  const query = SqlSchema.findOneOption({ Request, Result: JobRow, execute });
  return (value: Request["Type"]) =>
    query(value).pipe(Effect.catchTag("SchemaError", Effect.die), mapStoreError(operation));
};

const findAll = <Request extends Schema.Constraint>(
  operation: string,
  Request: Request,
  execute: (request: Request["Encoded"]) => Effect.Effect<ReadonlyArray<unknown>, SqlError>,
) => {
  const query = SqlSchema.findAll({ Request, Result: JobRow, execute });
  return (value: Request["Type"]) =>
    query(value).pipe(Effect.catchTag("SchemaError", Effect.die), mapStoreError(operation));
};

const makeCreate = (sql: SqlClient.SqlClient) => {
  const encodeAttached = Schema.encodeEffect(Schema.BooleanFromBit);
  const encode = Schema.encodeEffect(
    JobSubmission.pipe(
      Schema.encodeKeys({
        jobId: "job_id",
        sessionId: "session_id",
        stdoutPath: "stdout_path",
        stderrPath: "stderr_path",
        resultPath: "result_path",
      }),
    ),
  );
  return Effect.fn("SqliteJobStore.create")(function* (job: JobSubmission) {
    const submission = yield* encode(job).pipe(Effect.orDie);
    const attached = yield* encodeAttached(job.attached).pipe(Effect.orDie);
    const result = yield* sql`
      INSERT INTO jobs ${sql.insert({
        ...submission,
        attached,
        status: "Accepted",
      })}
      ON CONFLICT(job_id) DO NOTHING
      RETURNING job_id
    `.pipe(mapStoreError("create"));
    return result.length === 1;
  });
};

const makeBegin = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteJobStore.begin")(function* (jobId: JobId) {
    const rows = yield* sql`
      UPDATE jobs SET status = 'Starting'
      WHERE job_id = ${jobId} AND status = 'Accepted'
      RETURNING job_id
    `.pipe(mapStoreError("begin"));
    return rows.length === 1;
  });

const makeActivate = (sql: SqlClient.SqlClient) =>
  findOne(
    "activate",
    Schema.Struct({ jobId: JobId, ...ProcessIdentity.fields }),
    ({ jobId, pid, processGroupId, processStartId }) => sql`
      UPDATE jobs SET
        status = CASE WHEN status = 'Starting' THEN 'Running' ELSE status END,
        pid = ${pid}, process_group_id = ${processGroupId}, process_start_id = ${processStartId}
      WHERE job_id = ${jobId} AND status IN ('Starting', 'Stopping')
        AND pid IS NULL
      RETURNING *
    `,
  );

const makeRequestStop = (sql: SqlClient.SqlClient) =>
  findOne(
    "requestStop",
    JobAddress,
    ({ jobId, sessionId }) => sql`
    UPDATE jobs SET status = CASE WHEN status = 'Accepted' THEN 'Cancelled' ELSE 'Stopping' END
    WHERE job_id = ${jobId} AND session_id = ${sessionId}
      AND status IN ${sql.in(JobActiveStatus.literals)}
    RETURNING *
  `,
  );

const makeDetach = (sql: SqlClient.SqlClient) =>
  findOne(
    "detach",
    JobAddress,
    ({ jobId, sessionId }) => sql`
    UPDATE jobs SET attached = FALSE
    WHERE job_id = ${jobId} AND session_id = ${sessionId}
      AND status IN ${sql.in(JobActiveStatus.literals)}
    RETURNING *
  `,
  );

const completionSources = (outcome: JobOutcome): NonEmptyReadonlyArray<JobActiveStatus> => {
  return JobOutcome.match<NonEmptyReadonlyArray<JobActiveStatus>>(outcome, {
    Succeeded: () => JobStartedStatus.literals,
    Failed: () => JobStartedStatus.literals,
    Cancelled: () => JobActiveStatus.literals,
    Lost: () => JobStartedStatus.literals,
  });
};

interface CompletionValues {
  readonly status: JobTerminalStatus;
  readonly exitCode: Option.Option<number>;
  readonly detail: Option.Option<string>;
}

const makeComplete = (sql: SqlClient.SqlClient): JobStoreShape["complete"] =>
  Effect.fn("SqliteJobStore.complete")(function* (jobId, outcome) {
    const values = JobOutcome.match<CompletionValues>(outcome, {
      Succeeded: ({ exitCode }) => ({
        status: "Succeeded",
        exitCode: Option.some(exitCode),
        detail: Option.none<string>(),
      }),
      Failed: ({ exitCode, detail }) => ({ status: "Failed", exitCode, detail }),
      Cancelled: () => ({
        status: "Cancelled",
        exitCode: Option.none<number>(),
        detail: Option.none<string>(),
      }),
      Lost: ({ detail }) => ({
        status: "Lost",
        exitCode: Option.none<number>(),
        detail,
      }),
    });
    const rows = yield* sql`
      UPDATE jobs SET
        status = ${values.status},
        exit_code = ${Option.getOrNull(values.exitCode)},
        detail = ${Option.getOrNull(values.detail)}
      WHERE job_id = ${jobId} AND status IN ${sql.in(completionSources(outcome))}
      RETURNING job_id
    `.pipe(mapStoreError("complete"));
    return rows.length === 1;
  });

const makeListByStatus = (sql: SqlClient.SqlClient): JobStoreShape["listByStatus"] =>
  findAll(
    "listByStatus",
    Schema.NonEmptyArray(JobActiveStatus),
    (statuses) => sql`
      SELECT * FROM jobs WHERE status IN ${sql.in(statuses)} ORDER BY job_id
    `,
  );

const makeListAttached = (sql: SqlClient.SqlClient) =>
  findAll(
    "listAttachedActive",
    Schema.Struct({ sessionId: SessionId }),
    ({ sessionId }) => sql`
      SELECT * FROM jobs
      WHERE session_id = ${sessionId} AND attached = TRUE
        AND status IN ${sql.in(JobActiveStatus.literals)}
      ORDER BY job_id
    `,
  );

export const makeSqliteJobStore: Effect.Effect<JobStoreShape, never, SqlClient.SqlClient> =
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const find = findOne(
      "get",
      JobAddress,
      ({ jobId, sessionId }) => sql`
        SELECT * FROM jobs WHERE job_id = ${jobId} AND session_id = ${sessionId}
      `,
    );
    const listByStatus = makeListByStatus(sql);
    const listAttached = makeListAttached(sql);
    const activate = makeActivate(sql);
    return JobStore.of({
      create: makeCreate(sql),
      get: find,
      begin: makeBegin(sql),
      activate: (jobId, identity) => activate({ jobId, ...identity }),
      requestStop: makeRequestStop(sql),
      complete: makeComplete(sql),
      detach: makeDetach(sql),
      listRecoverable: listByStatus(["Running", "Stopping"]),
      listUncommitted: listByStatus(["Accepted", "Starting"]),
      listByStatus,
      listAttachedActive: (sessionId) => listAttached({ sessionId }),
    });
  });

export const layerSqliteJobStore = Layer.effect(JobStore, makeSqliteJobStore);
