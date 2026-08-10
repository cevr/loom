import { Schema } from "effect";

const identifier = (name: string) => Schema.NonEmptyString.pipe(Schema.brand(name));

export const SessionId = identifier("@cvr/loom/SessionId");
export type SessionId = typeof SessionId.Type;

export const WorkspaceRoot = identifier("@cvr/loom/WorkspaceRoot");
export type WorkspaceRoot = typeof WorkspaceRoot.Type;

export const AgentId = identifier("@cvr/loom/AgentId");
export type AgentId = typeof AgentId.Type;

export const CellId = identifier("@cvr/loom/CellId");
export type CellId = typeof CellId.Type;

export const JobId = identifier("@cvr/loom/JobId");
export type JobId = typeof JobId.Type;

export const WorkflowRunId = identifier("@cvr/loom/WorkflowRunId");
export type WorkflowRunId = typeof WorkflowRunId.Type;

export const WorkflowIncarnationId = identifier("@cvr/loom/WorkflowIncarnationId");
export type WorkflowIncarnationId = typeof WorkflowIncarnationId.Type;

export const WorkflowName = identifier("@cvr/loom/WorkflowName");
export type WorkflowName = typeof WorkflowName.Type;

export const WorkflowVersion = identifier("@cvr/loom/WorkflowVersion");
export type WorkflowVersion = typeof WorkflowVersion.Type;

export const WorkflowKey = identifier("@cvr/loom/WorkflowKey");
export type WorkflowKey = typeof WorkflowKey.Type;

export const WorkflowCapability = identifier("@cvr/loom/WorkflowCapability");
export type WorkflowCapability = typeof WorkflowCapability.Type;

export const WorkflowStepId = identifier("@cvr/loom/WorkflowStepId");
export type WorkflowStepId = typeof WorkflowStepId.Type;

export const WorkflowActivityKey = identifier("@cvr/loom/WorkflowActivityKey");
export type WorkflowActivityKey = typeof WorkflowActivityKey.Type;

export const workflowAgentId = (activityKey: WorkflowActivityKey): AgentId =>
  AgentId.make(`workflow-agent:${activityKey}`);

export const workflowJobId = (activityKey: WorkflowActivityKey): JobId =>
  JobId.make(`workflow-job:${activityKey}`);

export const WorkflowSignalName = identifier("@cvr/loom/WorkflowSignalName");
export type WorkflowSignalName = typeof WorkflowSignalName.Type;

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

export const ArtifactId = identifier("@cvr/loom/ArtifactId");
export type ArtifactId = typeof ArtifactId.Type;

export const workflowArtifactId = (activityKey: WorkflowActivityKey): ArtifactId =>
  ArtifactId.make(`workflow-artifact:${activityKey}`);

export const AgentOwner = Schema.Struct({
  sessionId: SessionId,
  agentId: AgentId,
});
export type AgentOwner = typeof AgentOwner.Type;

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
  parent: AgentParent,
  prompt: Schema.NonEmptyString,
  status: WorkflowChildAgentStatus,
});
export type WorkflowChildAgent = typeof WorkflowChildAgent.Type;

const positiveInteger = Schema.Int.check(Schema.isGreaterThan(0));

export const WorkflowBudget = Schema.Struct({
  maxSteps: positiveInteger,
  maxAgentRuns: positiveInteger,
  maxParallelism: positiveInteger,
  maxInlineStepResultBytes: positiveInteger,
  maxTokens: Schema.OptionFromNullOr(positiveInteger),
  maxDurationMillis: Schema.OptionFromNullOr(positiveInteger),
});
export type WorkflowBudget = typeof WorkflowBudget.Type;

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

export const WorkflowRunAddress = Schema.Struct({
  sessionId: SessionId,
  workflowRunId: WorkflowRunId,
});
export type WorkflowRunAddress = typeof WorkflowRunAddress.Type;

export const WorkflowRunExecution = Schema.Struct({
  incarnationId: WorkflowIncarnationId,
  request: WorkflowRunRequest,
});
export type WorkflowRunExecution = typeof WorkflowRunExecution.Type;

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

export const JobOutcome = Schema.TaggedUnion({
  Succeeded: { exitCode: Schema.Literal(0) },
  Failed: {
    exitCode: Schema.OptionFromNullOr(Schema.Int),
    detail: Schema.OptionFromNullOr(Schema.String),
  },
  Cancelled: {},
  Lost: { detail: Schema.OptionFromNullOr(Schema.String) },
});
export type JobOutcome = typeof JobOutcome.Type;

export const JobRecord = Schema.Struct({
  jobId: JobId,
  sessionId: SessionId,
  command: Schema.NonEmptyString,
  attached: Schema.Boolean,
  status: JobStatus,
  stdoutPath: Schema.NonEmptyString,
  stderrPath: Schema.NonEmptyString,
  resultPath: Schema.NonEmptyString,
  identity: Schema.OptionFromNullOr(ProcessIdentity),
  exitCode: Schema.OptionFromNullOr(Schema.Int),
  detail: Schema.OptionFromNullOr(Schema.String),
});
export type JobRecord = typeof JobRecord.Type;

export const JobAddress = Schema.Struct({
  sessionId: SessionId,
  jobId: JobId,
});
export type JobAddress = typeof JobAddress.Type;
