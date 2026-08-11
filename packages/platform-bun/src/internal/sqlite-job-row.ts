import {
  JobFailure,
  JobFailureExitCode,
  JobId,
  JobRecord,
  JobSubmission,
  ProcessIdentity,
  SessionId,
} from "@cvr/loom-domain";
import { Option, Schema, SchemaTransformation } from "effect";

const JobRowFields = {
  jobId: JobId,
  sessionId: SessionId,
  command: JobSubmission.fields.command,
  attached: Schema.BooleanFromBit,
  stdoutPath: JobSubmission.fields.stdoutPath,
  stderrPath: JobSubmission.fields.stderrPath,
  resultPath: JobSubmission.fields.resultPath,
};
const JobColumnNames = {
  jobId: "job_id",
  sessionId: "session_id",
  stdoutPath: "stdout_path",
  stderrPath: "stderr_path",
  resultPath: "result_path",
};
const Absent = Schema.OptionFromNullOr(Schema.Never);
const NoIdentityFields = {
  pid: Absent,
  processGroupId: Absent,
  processStartId: Absent,
};
const NoOutcomeFields = {
  failureKind: Absent,
  exitCode: Absent,
  detail: Absent,
};
const jobRow = <const Fields extends Schema.Struct.Fields>(fields: Fields) =>
  Schema.Struct({ ...JobRowFields, ...fields }).pipe(
    Schema.encodeKeys({
      ...JobColumnNames,
      processGroupId: "process_group_id",
      processStartId: "process_start_id",
      failureKind: "failure_kind",
      exitCode: "exit_code",
    }),
  );

const FlatJob = Schema.Union([
  jobRow({ status: Schema.Literal("Accepted"), ...NoIdentityFields, ...NoOutcomeFields }),
  jobRow({ status: Schema.Literal("Starting"), ...NoIdentityFields, ...NoOutcomeFields }),
  jobRow({ status: Schema.Literal("Running"), ...ProcessIdentity.fields, ...NoOutcomeFields }),
  jobRow({ status: Schema.Literal("Stopping"), ...NoIdentityFields, ...NoOutcomeFields }),
  jobRow({ status: Schema.Literal("Stopping"), ...ProcessIdentity.fields, ...NoOutcomeFields }),
  jobRow({
    status: Schema.Literal("Succeeded"),
    ...NoIdentityFields,
    failureKind: Absent,
    exitCode: Schema.Literal(0),
    detail: Absent,
  }),
  jobRow({
    status: Schema.Literal("Failed"),
    ...NoIdentityFields,
    failureKind: Schema.Literal("Launch"),
    exitCode: Absent,
    detail: Schema.NonEmptyString,
  }),
  jobRow({
    status: Schema.Literal("Failed"),
    ...NoIdentityFields,
    failureKind: Schema.Literal("Exit"),
    exitCode: JobFailureExitCode,
    detail: Schema.OptionFromNullOr(Schema.String),
  }),
  jobRow({
    status: Schema.Literal("Failed"),
    ...NoIdentityFields,
    failureKind: Schema.Literal("Runtime"),
    exitCode: Absent,
    detail: Schema.NonEmptyString,
  }),
  jobRow({ status: Schema.Literal("Cancelled"), ...NoIdentityFields, ...NoOutcomeFields }),
  jobRow({
    status: Schema.Literal("Lost"),
    ...NoIdentityFields,
    failureKind: Absent,
    exitCode: Absent,
    detail: Schema.OptionFromNullOr(Schema.String),
  }),
]);

const submissionFromRow = (row: typeof FlatJob.Type) => ({
  jobId: row.jobId,
  sessionId: row.sessionId,
  command: row.command,
  attached: row.attached,
  stdoutPath: row.stdoutPath,
  stderrPath: row.stderrPath,
  resultPath: row.resultPath,
});

type FlatStoppingJob = Extract<typeof FlatJob.Type, { readonly status: "Stopping" }>;

const hasStoppingIdentity = (
  row: FlatStoppingJob,
): row is Extract<FlatStoppingJob, { readonly pid: number }> => !Option.isOption(row.pid);

const stoppingIdentity = (row: FlatStoppingJob) => {
  if (!hasStoppingIdentity(row)) return Option.none();
  return Option.some(ProcessIdentity.make(row));
};

const failureFromRow = (row: Extract<typeof FlatJob.Type, { readonly status: "Failed" }>) => {
  switch (row.failureKind) {
    case "Launch":
      return JobFailure.cases.Launch.make({ detail: row.detail });
    case "Exit":
      return JobFailure.cases.Exit.make({ exitCode: row.exitCode, detail: row.detail });
    case "Runtime":
      return JobFailure.cases.Runtime.make({ detail: row.detail });
  }
};

const decodeJobRow = (row: typeof FlatJob.Type): JobRecord => {
  const submission = submissionFromRow(row);
  switch (row.status) {
    case "Accepted":
      return JobRecord.cases.Accepted.make(submission);
    case "Starting":
      return JobRecord.cases.Starting.make(submission);
    case "Running":
      return JobRecord.cases.Running.make({ ...submission, identity: ProcessIdentity.make(row) });
    case "Stopping":
      return JobRecord.cases.Stopping.make({ ...submission, identity: stoppingIdentity(row) });
    case "Succeeded":
      return JobRecord.cases.Succeeded.make({ ...submission, exitCode: 0 });
    case "Failed":
      return JobRecord.cases.Failed.make({ ...submission, failure: failureFromRow(row) });
    case "Cancelled":
      return JobRecord.cases.Cancelled.make(submission);
    case "Lost":
      return JobRecord.cases.Lost.make({ ...submission, detail: row.detail });
  }
};

const emptyJobRow = <Job extends JobRecord>(job: Job) => ({
  ...job,
  pid: Option.none<never>(),
  processGroupId: Option.none<never>(),
  processStartId: Option.none<never>(),
  failureKind: Option.none<never>(),
  exitCode: Option.none<never>(),
  detail: Option.none<never>(),
});

const encodeJobRow = (job: JobRecord): typeof FlatJob.Type => {
  switch (job.status) {
    case "Running":
      return { ...emptyJobRow(job), ...job.identity };
    case "Stopping":
      return Option.match(job.identity, {
        onNone: () => emptyJobRow(job),
        onSome: (identity) => ({ ...emptyJobRow(job), ...identity }),
      });
    case "Succeeded":
      return { ...emptyJobRow(job), exitCode: 0 };
    case "Failed":
      return JobFailure.match<typeof FlatJob.Type>(job.failure, {
        Launch: (failure) => ({
          ...emptyJobRow(job),
          failureKind: "Launch",
          detail: failure.detail,
        }),
        Exit: (failure) => ({
          ...emptyJobRow(job),
          failureKind: "Exit",
          exitCode: failure.exitCode,
          detail: failure.detail,
        }),
        Runtime: (failure) => ({
          ...emptyJobRow(job),
          failureKind: "Runtime",
          detail: failure.detail,
        }),
      });
    case "Lost":
      return { ...emptyJobRow(job), detail: job.detail };
    case "Accepted":
    case "Starting":
    case "Cancelled":
      return emptyJobRow(job);
  }
};

export const JobRow = FlatJob.pipe(
  Schema.decodeTo(
    Schema.toType(JobRecord),
    SchemaTransformation.transform({ decode: decodeJobRow, encode: encodeJobRow }),
  ),
);

export const JobAcceptedRow = JobRow.pipe(Schema.refine(JobRecord.guards.Accepted));
export const JobStartingRow = JobRow.pipe(Schema.refine(JobRecord.guards.Starting));
export const JobRecoverableRow = JobRow.pipe(
  Schema.refine(JobRecord.isAnyOf(["Running", "Stopping"])),
);
export const JobTerminalRow = JobRow.pipe(
  Schema.refine(JobRecord.isAnyOf(["Succeeded", "Failed", "Cancelled", "Lost"])),
);
export const JobUncommittedRow = JobRow.pipe(
  Schema.refine(JobRecord.isAnyOf(["Accepted", "Starting"])),
);

export const JobSubmissionRow = Schema.Struct({
  ...JobSubmission.fields,
  attached: Schema.BooleanFromBit,
}).pipe(Schema.encodeKeys(JobColumnNames));
