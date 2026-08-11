import {
  JobActiveStatus,
  JobAddress,
  JobFailure,
  JobFailureExitCode,
  JobId,
  JobOutcome,
  JobStartedStatus,
  JobTerminalStatus,
  ProcessIdentity,
  SessionId,
} from "@cvr/loom-domain";
import { JobStore, JobStoreError, type JobStoreShape } from "@cvr/loom-runtime";
import { Effect, Layer, Option, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";
import type { SqlError } from "effect/unstable/sql/SqlError";
import {
  JobAcceptedRow,
  JobRecoverableRow,
  JobRow,
  JobStartingRow,
  JobSubmissionRow,
  JobTerminalRow,
  JobUncommittedRow,
} from "./sqlite-job-row.js";

const mapStoreError = (operation: string) =>
  Effect.mapError((cause: SqlError) => new JobStoreError({ operation, cause }));

const findOne = <Request extends Schema.Constraint, Result extends Schema.Constraint>(
  operation: string,
  Request: Request,
  Result: Result,
  execute: (request: Request["Encoded"]) => Effect.Effect<ReadonlyArray<unknown>, SqlError>,
) => {
  const query = SqlSchema.findOneOption({ Request, Result, execute });
  return (value: Request["Type"]) =>
    query(value).pipe(Effect.catchTag("SchemaError", Effect.die), mapStoreError(operation));
};

const findAll = <Request extends Schema.Constraint, Result extends Schema.Constraint>(
  operation: string,
  Request: Request,
  Result: Result,
  execute: (request: Request["Encoded"]) => Effect.Effect<ReadonlyArray<unknown>, SqlError>,
) => {
  const query = SqlSchema.findAll({ Request, Result, execute });
  return (value: Request["Type"]) =>
    query(value).pipe(Effect.catchTag("SchemaError", Effect.die), mapStoreError(operation));
};

const makeCreate = (sql: SqlClient.SqlClient) => {
  const create = findOne(
    "create",
    JobSubmissionRow,
    JobAcceptedRow,
    (submission) => sql`
      INSERT INTO jobs ${sql.insert({ ...submission, status: "Accepted" })}
      ON CONFLICT(job_id) DO NOTHING
      RETURNING *
    `,
  );
  return create;
};

const makeBegin = (sql: SqlClient.SqlClient) =>
  findOne(
    "begin",
    Schema.Struct({ jobId: JobId }),
    JobStartingRow,
    ({ jobId }) => sql`
      UPDATE jobs SET
        status = 'Starting',
        pid = NULL,
        process_group_id = NULL,
        process_start_id = NULL,
        failure_kind = NULL,
        exit_code = NULL,
        detail = NULL
      WHERE job_id = ${jobId}
        AND (status = 'Accepted' OR (status = 'Failed' AND failure_kind = 'Launch'))
      RETURNING *
    `,
  );

const makeActivate = (sql: SqlClient.SqlClient) =>
  findOne(
    "activate",
    Schema.Struct({ jobId: JobId, ...ProcessIdentity.fields }),
    JobRow,
    ({ jobId, pid, processGroupId, processStartId }) => sql`
      UPDATE jobs SET
        status = CASE WHEN status = 'Starting' THEN 'Running' ELSE status END,
        pid = ${pid}, process_group_id = ${processGroupId}, process_start_id = ${processStartId}
      WHERE job_id = ${jobId}
        AND (status = 'Starting' OR (status = 'Stopping' AND pid IS NULL))
      RETURNING *
    `,
  );

const makeRequestStop = (sql: SqlClient.SqlClient) =>
  findOne(
    "requestStop",
    JobAddress,
    JobRow,
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
    JobRow,
    ({ jobId, sessionId }) => sql`
    UPDATE jobs SET attached = FALSE
    WHERE job_id = ${jobId} AND session_id = ${sessionId}
      AND status IN ${sql.in(JobActiveStatus.literals)}
    RETURNING *
  `,
  );

interface CompletionValues {
  readonly status: JobTerminalStatus;
  readonly failureKind: Option.Option<JobFailure["_tag"]>;
  readonly exitCode: Option.Option<0 | JobFailureExitCode>;
  readonly detail: Option.Option<string>;
}

const completionValues = (outcome: JobOutcome): CompletionValues =>
  JobOutcome.match<CompletionValues>(outcome, {
    Succeeded: ({ exitCode }) => ({
      status: "Succeeded",
      failureKind: Option.none(),
      exitCode: Option.some(exitCode),
      detail: Option.none(),
    }),
    Failed: ({ failure }) =>
      JobFailure.match<CompletionValues>(failure, {
        Launch: ({ detail }) => ({
          status: "Failed",
          failureKind: Option.some("Launch"),
          exitCode: Option.none(),
          detail: Option.some(detail),
        }),
        Exit: ({ exitCode, detail }) => ({
          status: "Failed",
          failureKind: Option.some("Exit"),
          exitCode: Option.some(exitCode),
          detail,
        }),
        Runtime: ({ detail }) => ({
          status: "Failed",
          failureKind: Option.some("Runtime"),
          exitCode: Option.none(),
          detail: Option.some(detail),
        }),
      }),
    Cancelled: () => ({
      status: "Cancelled",
      failureKind: Option.none(),
      exitCode: Option.none(),
      detail: Option.none(),
    }),
    Lost: ({ detail }) => ({
      status: "Lost",
      failureKind: Option.none(),
      exitCode: Option.none(),
      detail,
    }),
  });

const CompletionRequest = Schema.Struct({
  jobId: JobId,
  status: JobTerminalStatus,
  failureKind: Schema.OptionFromNullOr(Schema.Literals(["Launch", "Exit", "Runtime"])),
  exitCode: Schema.OptionFromNullOr(Schema.Union([Schema.Literal(0), JobFailureExitCode])),
  detail: Schema.OptionFromNullOr(Schema.String),
});

const makeComplete = (sql: SqlClient.SqlClient): JobStoreShape["complete"] => {
  const complete = findOne(
    "complete",
    CompletionRequest,
    JobTerminalRow,
    ({ jobId, status, failureKind, exitCode, detail }) => sql`
      UPDATE jobs SET
        status = ${status},
        pid = NULL,
        process_group_id = NULL,
        process_start_id = NULL,
        failure_kind = ${failureKind},
        exit_code = ${exitCode},
        detail = ${detail}
      WHERE job_id = ${jobId} AND status IN ${sql.in(JobStartedStatus.literals)}
      RETURNING *
    `,
  );
  return (jobId, outcome) => complete({ jobId, ...completionValues(outcome) });
};

const makeListByStatus = <Result extends Schema.Constraint>(
  sql: SqlClient.SqlClient,
  Result: Result,
) =>
  findAll(
    "listByStatus",
    Schema.NonEmptyArray(JobActiveStatus),
    Result,
    (statuses) => sql`
      SELECT * FROM jobs WHERE status IN ${sql.in(statuses)} ORDER BY job_id
    `,
  );

const makeListAttached = (sql: SqlClient.SqlClient) =>
  findAll(
    "listAttachedActive",
    Schema.Struct({ sessionId: SessionId }),
    JobRow,
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
      JobRow,
      ({ jobId, sessionId }) => sql`
        SELECT * FROM jobs WHERE job_id = ${jobId} AND session_id = ${sessionId}
      `,
    );
    const listRecoverable = makeListByStatus(sql, JobRecoverableRow);
    const listUncommitted = makeListByStatus(sql, JobUncommittedRow);
    const listAttached = makeListAttached(sql);
    const activate = makeActivate(sql);
    const begin = makeBegin(sql);
    return JobStore.of({
      create: makeCreate(sql),
      get: find,
      begin: (jobId) => begin({ jobId }),
      activate: (jobId, identity) => activate({ jobId, ...identity }),
      requestStop: makeRequestStop(sql),
      complete: makeComplete(sql),
      detach: makeDetach(sql),
      listRecoverable: listRecoverable(["Running", "Stopping"]),
      listUncommitted: listUncommitted(["Accepted", "Starting"]),
      listAttachedActive: (sessionId) => listAttached({ sessionId }),
    });
  });

export const layerSqliteJobStore = Layer.effect(JobStore, makeSqliteJobStore);
