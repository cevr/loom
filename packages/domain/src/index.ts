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

export const CellJournalEntry = Schema.Struct({
  sessionId: SessionId,
  agentId: AgentId,
  cellId: CellId,
  source: Schema.String,
});
export type CellJournalEntry = typeof CellJournalEntry.Type;

export const JobId = identifier("@cvr/loom/JobId");
export type JobId = typeof JobId.Type;

export const WorkflowRunId = identifier("@cvr/loom/WorkflowRunId");
export type WorkflowRunId = typeof WorkflowRunId.Type;

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

export const WorkflowSignalName = identifier("@cvr/loom/WorkflowSignalName");
export type WorkflowSignalName = typeof WorkflowSignalName.Type;

export const WorkflowSignalAddress = Schema.Struct({
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

export const AcceptedWorkflowRun = Schema.Struct({
  identity: WorkflowIdentity,
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

export const JobProcessStatus = Schema.Literals([
  "Running",
  "Stopping",
  "Recovered",
  "ExitedWhileOffline",
  "IdentityMismatch",
]);
export type JobProcessStatus = typeof JobProcessStatus.Type;

export const JobProcessRecord = Schema.Struct({
  jobId: JobId,
  sessionId: SessionId,
  identity: ProcessIdentity,
  stdoutPath: Schema.NonEmptyString,
  stderrPath: Schema.NonEmptyString,
  status: JobProcessStatus,
  recoveryDetail: Schema.OptionFromNullOr(Schema.String),
});
export type JobProcessRecord = typeof JobProcessRecord.Type;
