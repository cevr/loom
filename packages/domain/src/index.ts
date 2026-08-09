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

export const ArtifactId = identifier("@cvr/loom/ArtifactId");
export type ArtifactId = typeof ArtifactId.Type;

export const AgentOwner = Schema.Struct({
  sessionId: SessionId,
  agentId: AgentId,
});
export type AgentOwner = typeof AgentOwner.Type;

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

const positiveInteger = Schema.Int.check(Schema.isGreaterThan(0));

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
  recoveryDetail: Schema.NullOr(Schema.String),
});
export type JobProcessRecord = typeof JobProcessRecord.Type;
