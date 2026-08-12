import { Effect, Option, Schema } from "effect";
import {
  AgentId,
  JobId,
  SessionId,
  WorkflowActivityKey,
  WorkflowCapability,
  WorkflowIncarnationId,
  WorkflowKey,
  WorkflowName,
  WorkflowRunId,
  WorkflowSignalName,
  WorkflowVersion,
} from "./identifiers.js";

export * from "./identifiers.js";
export * from "./goal.js";

export const WorkflowSignalAddress = Schema.Struct({
  sessionId: SessionId,
  workflowRunId: WorkflowRunId,
  name: WorkflowSignalName,
});
export type WorkflowSignalAddress = typeof WorkflowSignalAddress.Type;

export const WorkflowRequestDigest = Schema.String.check(
  Schema.isPattern(/^sha256:[0-9a-f]{64}$/u),
).pipe(Schema.brand("@cvr/loom/WorkflowRequestDigest"));
export type WorkflowRequestDigest = typeof WorkflowRequestDigest.Type;

export const WorkflowIdentity = Schema.Struct({
  sessionId: SessionId,
  name: WorkflowName,
  version: WorkflowVersion,
  key: WorkflowKey,
});
export type WorkflowIdentity = typeof WorkflowIdentity.Type;

export const AgentOwner = Schema.Struct({
  sessionId: SessionId,
  agentId: AgentId,
});
export type AgentOwner = typeof AgentOwner.Type;

export { PluginStateAddress, PluginStateScope } from "./plugin-state.js";

export const WorkflowRunAddress = Schema.Struct({
  sessionId: SessionId,
  workflowRunId: WorkflowRunId,
});
export type WorkflowRunAddress = typeof WorkflowRunAddress.Type;

export const AgentParent = Schema.TaggedUnion({
  Session: {
    sessionId: SessionId,
  },
  Agent: {
    sessionId: SessionId,
    agentId: AgentId,
  },
  WorkflowRun: {
    sessionId: SessionId,
    workflowRunId: WorkflowRunId,
  },
});
export type AgentParent = typeof AgentParent.Type;

export const WorkflowChildAgentStatus = Schema.Literals(["Active", "Stopped"]);
export type WorkflowChildAgentStatus = typeof WorkflowChildAgentStatus.Type;

export const WorkflowChildAgent = Schema.Struct({
  activityKey: WorkflowActivityKey,
  agentId: AgentId,
  jobId: JobId,
  parent: WorkflowRunAddress,
  prompt: Schema.NonEmptyString,
  status: WorkflowChildAgentStatus,
});
export type WorkflowChildAgent = typeof WorkflowChildAgent.Type;

const positiveInteger = Schema.Int.check(Schema.isGreaterThan(0));

const positiveIntegerWithDefault = (value: number) =>
  positiveInteger.pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(value)),
    Schema.withConstructorDefault(Effect.succeed(value)),
  );

const noPositiveInteger = Effect.succeed(Option.none<number>());

const optionalPositiveInteger = Schema.OptionFromNullOr(positiveInteger).pipe(
  Schema.withDecodingDefaultTypeKey(noPositiveInteger),
  Schema.withConstructorDefault(noPositiveInteger),
);

export const WorkflowBudget = Schema.Struct({
  maxSteps: positiveIntegerWithDefault(32),
  maxAgentRuns: positiveIntegerWithDefault(8),
  maxParallelism: positiveIntegerWithDefault(4),
  maxInlineStepResultBytes: positiveIntegerWithDefault(64 * 1_024),
  maxTokens: optionalPositiveInteger,
  maxDurationMillis: optionalPositiveInteger,
});
export type WorkflowBudget = typeof WorkflowBudget.Type;

export const defaultWorkflowBudget = WorkflowBudget.make({});

export const WorkflowDefinition = Schema.Struct({
  name: WorkflowName,
  version: WorkflowVersion,
  interpreterVersion: positiveInteger,
  source: Schema.NonEmptyString,
  capabilities: Schema.Array(WorkflowCapability),
  signals: Schema.Array(WorkflowSignalName),
});
export type WorkflowDefinition = typeof WorkflowDefinition.Type;

export const WorkflowRunRequest = Schema.Struct({
  sessionId: SessionId,
  key: WorkflowKey,
  definition: WorkflowDefinition,
  input: Schema.Json,
  budget: WorkflowBudget,
});
export type WorkflowRunRequest = typeof WorkflowRunRequest.Type;

export const WorkflowRunExecution = Schema.Struct({
  incarnationId: WorkflowIncarnationId,
  request: WorkflowRunRequest,
});
export type WorkflowRunExecution = typeof WorkflowRunExecution.Type;

export const WorkflowRunAcceptanceStatus = Schema.Literals(["Active", "Retiring"]);
export type WorkflowRunAcceptanceStatus = typeof WorkflowRunAcceptanceStatus.Type;

export const AcceptedWorkflowRun = Schema.Struct({
  incarnationId: WorkflowIncarnationId,
  workflowRunId: WorkflowRunId,
  request: WorkflowRunRequest,
  digest: WorkflowRequestDigest,
});
export type AcceptedWorkflowRun = typeof AcceptedWorkflowRun.Type;

export const ActorSubject = Schema.TaggedUnion({
  Agent: {
    sessionId: SessionId,
    agentId: AgentId,
  },
  Job: {
    sessionId: SessionId,
    jobId: JobId,
  },
  WorkflowRun: {
    sessionId: SessionId,
    workflowRunId: WorkflowRunId,
  },
});
export type ActorSubject = typeof ActorSubject.Type;

export const ActorActivity = Schema.TaggedUnion({
  Idle: {},
  Working: {
    message: Schema.optionalKey(Schema.String),
  },
  Blocked: {
    message: Schema.String,
  },
  Failed: {
    message: Schema.String,
  },
  Stopped: {},
});
export type ActorActivity = typeof ActorActivity.Type;

export const ActorStateProjection = Schema.Struct({
  subject: ActorSubject,
  activity: ActorActivity,
  revision: Schema.Natural,
});
export type ActorStateProjection = typeof ActorStateProjection.Type;

export const ProcessIdentity = Schema.Struct({
  pid: positiveInteger,
  processGroupId: positiveInteger,
  processStartId: Schema.NonEmptyString,
});
export type ProcessIdentity = typeof ProcessIdentity.Type;

export const processIdentitiesMatch = Schema.toEquivalence(ProcessIdentity);

export const CodeKernelProcessRecord = Schema.Struct({
  ...AgentOwner.fields,
  ...ProcessIdentity.fields,
});
export type CodeKernelProcessRecord = typeof CodeKernelProcessRecord.Type;

export const JobActiveStatus = Schema.Literals(["Accepted", "Starting", "Running", "Stopping"]);
export type JobActiveStatus = typeof JobActiveStatus.Type;

export const JobStartedStatus = JobActiveStatus.pick(["Starting", "Running", "Stopping"]);
export type JobStartedStatus = typeof JobStartedStatus.Type;

export const JobTerminalStatus = Schema.Literals(["Succeeded", "Failed", "Cancelled", "Lost"]);
export type JobTerminalStatus = typeof JobTerminalStatus.Type;

export const JobStatus = Schema.Union([JobActiveStatus, JobTerminalStatus]);
export type JobStatus = typeof JobStatus.Type;

export const JobSubmission = Schema.Struct({
  jobId: JobId,
  sessionId: SessionId,
  command: Schema.NonEmptyString,
  attached: Schema.Boolean,
  stdoutPath: Schema.NonEmptyString,
  stderrPath: Schema.NonEmptyString,
  resultPath: Schema.NonEmptyString,
});
export type JobSubmission = typeof JobSubmission.Type;

export const JobRequest = Schema.Struct({
  jobId: JobId,
  sessionId: SessionId,
  command: Schema.NonEmptyString,
  attached: Schema.Boolean,
});
export type JobRequest = typeof JobRequest.Type;

export const JobFailureExitCode = Schema.Union([
  Schema.Int.check(Schema.isLessThan(0)),
  Schema.Int.check(Schema.isGreaterThan(0)),
]);
export type JobFailureExitCode = typeof JobFailureExitCode.Type;

export const JobFailure = Schema.TaggedUnion({
  Launch: { detail: Schema.NonEmptyString },
  Exit: { exitCode: JobFailureExitCode, detail: Schema.OptionFromNullOr(Schema.String) },
  Runtime: { detail: Schema.NonEmptyString },
});
export type JobFailure = typeof JobFailure.Type;

export const JobOutcome = Schema.TaggedUnion({
  Succeeded: { exitCode: Schema.Literal(0) },
  Failed: { failure: JobFailure },
  Cancelled: {},
  Lost: { detail: Schema.OptionFromNullOr(Schema.String) },
});
export type JobOutcome = typeof JobOutcome.Type;

const JobRecordFields = JobSubmission.fields;

export const JobRecord = Schema.Union([
  Schema.Struct({ ...JobRecordFields, status: Schema.tag("Accepted") }),
  Schema.Struct({ ...JobRecordFields, status: Schema.tag("Starting") }),
  Schema.Struct({
    ...JobRecordFields,
    status: Schema.tag("Running"),
    identity: ProcessIdentity,
  }),
  Schema.Struct({
    ...JobRecordFields,
    status: Schema.tag("Stopping"),
    identity: Schema.OptionFromNullOr(ProcessIdentity),
  }),
  Schema.Struct({
    ...JobRecordFields,
    status: Schema.tag("Succeeded"),
    exitCode: Schema.Literal(0),
  }),
  Schema.Struct({
    ...JobRecordFields,
    status: Schema.tag("Failed"),
    failure: JobFailure,
  }),
  Schema.Struct({ ...JobRecordFields, status: Schema.tag("Cancelled") }),
  Schema.Struct({
    ...JobRecordFields,
    status: Schema.tag("Lost"),
    detail: Schema.OptionFromNullOr(Schema.String),
  }),
]).pipe(Schema.toTaggedUnion("status"));
export type JobRecord = typeof JobRecord.Type;
export type JobAcceptedRecord = Extract<JobRecord, { readonly status: "Accepted" }>;
export type JobStartingRecord = Extract<JobRecord, { readonly status: "Starting" }>;
export type JobUncommittedRecord = Extract<JobRecord, { readonly status: "Accepted" | "Starting" }>;
export type JobRecoverableRecord = Extract<JobRecord, { readonly status: "Running" | "Stopping" }>;
export type JobTerminalRecord = Extract<JobRecord, { readonly status: JobTerminalStatus }>;

export const JobAddress = Schema.Struct({
  sessionId: SessionId,
  jobId: JobId,
});
export type JobAddress = typeof JobAddress.Type;
