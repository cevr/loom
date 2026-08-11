import {
  JobFailure,
  JobAddress,
  JobOutcome,
  JobRequest,
  workflowAgentJobId,
  workflowJobId,
  type JobRecord,
} from "@cvr/loom-domain";
import {
  JobRuntime,
  SessionLifecycle,
  type SessionLifecycleShape,
  WorkflowAgentInput,
  WorkflowCapabilityExecutor,
  WorkflowChildAgentStore,
  WorkflowJobHandle,
  WorkflowJobInput,
  WorkflowStepExecution,
  supportsBuiltInWorkflowCapability,
  workflowAgentCapability,
  workflowJobCapability,
  type WorkflowActivityContext,
  type WorkflowChildAgentStoreShape,
  type WorkflowStepCall,
} from "@cvr/loom-runtime";
import { SessionClosingError, WorkflowStepError } from "@cvr/loom-protocol";
import { Effect, FileSystem, Inspectable, Layer, Option, Schema } from "effect";
import {
  layerBunWorkflowArtifactStore,
  type BunWorkflowArtifactStoreConfig,
} from "./bun-workflow-artifact-store.js";
import {
  awaitWorkflowAgent,
  type BunWorkflowAgentConfig,
  prepareWorkflowAgent,
} from "./bun-workflow-agent.js";

const decodeAgentInput = Schema.decodeUnknownEffect(WorkflowAgentInput);
const decodeJobInput = Schema.decodeUnknownEffect(WorkflowJobInput);

const stepError = (call: WorkflowStepCall, message: string) =>
  new WorkflowStepError({
    stepId: call.stepId,
    capability: call.capability,
    message,
  });

const decodeInput = <A>(
  call: WorkflowStepCall,
  decode: (input: Schema.Json) => Effect.Effect<A, Schema.SchemaError>,
) => decode(call.input).pipe(Effect.mapError((cause) => stepError(call, cause.message)));

const launchAgent = Effect.fn("WorkflowCapabilities.launchAgent")(function* (
  config: BunWorkflowAgentConfig,
  sessions: SessionLifecycleShape,
  store: WorkflowChildAgentStoreShape,
  jobs: JobRuntime["Service"],
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
) {
  const input = yield* decodeInput(call, decodeAgentInput);
  const agent = yield* sessions.admit(
    context.sessionId,
    store.claim(context, input.prompt).pipe(
      Effect.tap((claimed) =>
        prepareWorkflowAgent(config, claimed).pipe(Effect.flatMap(jobs.start)),
      ),
      Effect.mapError((error) => stepError(call, error.message)),
    ),
  );
  const result = yield* awaitWorkflowAgent(config, jobs, agent).pipe(
    Effect.mapError((error) => stepError(call, Inspectable.toStringUnknown(error.cause))),
  );
  // Issue 52 will replace this close-time classification with an Interrupted run state.
  if (
    JobOutcome.guards.Cancelled(result.outcome) &&
    (yield* sessions.isClosing(context.sessionId))
  ) {
    return yield* new SessionClosingError({ sessionId: context.sessionId });
  }
  yield* store
    .stop(context.activityKey)
    .pipe(Effect.mapError((error) => stepError(call, error.message)));
  return WorkflowStepExecution.make({
    value: result,
    tokenCount: 0,
    agentRuns: 1,
  });
});

const completeJob = Effect.fn("WorkflowCapabilities.completeJob")(function* (
  sessions: SessionLifecycleShape,
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
  jobId: ReturnType<typeof workflowJobId>,
  terminal: Option.Option<JobRecord>,
) {
  if (Option.isNone(terminal)) return yield* stepError(call, "The Job record is missing.");
  const job = terminal.value;
  if (job.status === "Failed") {
    const message = JobFailure.match(job.failure, {
      Launch: ({ detail }) => detail,
      Exit: ({ exitCode, detail }) =>
        Option.getOrElse(detail, () => `The Job exited with code ${exitCode}.`),
      Runtime: ({ detail }) => detail,
    });
    return yield* stepError(call, message);
  }
  if (job.status === "Lost") {
    return yield* stepError(
      call,
      Option.getOrElse(job.detail, () => "The Job was lost."),
    );
  }
  if (job.status === "Cancelled") {
    // Issue 52 will replace this close-time classification with an Interrupted run state.
    if (yield* sessions.isClosing(context.sessionId)) {
      return yield* new SessionClosingError({ sessionId: context.sessionId });
    }
    return yield* stepError(call, "The Job was cancelled.");
  }
  return WorkflowStepExecution.make({
    value: WorkflowJobHandle.make({ jobId }),
    tokenCount: 0,
    agentRuns: 0,
  });
});

const launchJob = Effect.fn("WorkflowCapabilities.launchJob")(function* (
  sessions: SessionLifecycleShape,
  jobs: JobRuntime["Service"],
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
) {
  const input = yield* decodeInput(call, decodeJobInput);
  const jobId = workflowJobId(context.activityKey);
  yield* sessions.admit(
    context.sessionId,
    jobs
      .start(
        JobRequest.make({
          jobId,
          sessionId: context.sessionId,
          command: input.command,
          attached: true,
        }),
      )
      .pipe(Effect.mapError((error) => stepError(call, Inspectable.toStringUnknown(error.cause)))),
  );
  const terminal = yield* jobs
    .awaitTerminal(JobAddress.make({ jobId, sessionId: context.sessionId }))
    .pipe(Effect.mapError((error) => stepError(call, Inspectable.toStringUnknown(error.cause))));
  return yield* completeJob(sessions, call, context, jobId, terminal);
});

export interface WorkflowCapabilitiesConfig
  extends BunWorkflowArtifactStoreConfig, BunWorkflowAgentConfig {}

const compensateAgent = Effect.fn("WorkflowCapabilities.compensateAgent")(function* (
  store: WorkflowChildAgentStoreShape,
  jobs: JobRuntime["Service"],
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
) {
  yield* jobs
    .cancel(
      JobAddress.make({
        jobId: workflowAgentJobId(context.activityKey),
        sessionId: context.sessionId,
      }),
    )
    .pipe(
      Effect.asVoid,
      Effect.mapError((error) => stepError(call, Inspectable.toStringUnknown(error.cause))),
    );
  yield* store
    .stop(context.activityKey)
    .pipe(Effect.mapError((error) => stepError(call, error.message)));
});

const makeCapabilityExecutor = (config: WorkflowCapabilitiesConfig) =>
  Effect.gen(function* () {
    const childAgents = yield* WorkflowChildAgentStore;
    const jobs = yield* JobRuntime;
    const sessions = yield* SessionLifecycle;
    const fs = yield* FileSystem.FileSystem;
    return WorkflowCapabilityExecutor.of({
      supports: supportsBuiltInWorkflowCapability,
      execute: (call, context) => {
        if (call.capability === workflowAgentCapability) {
          return launchAgent(config, sessions, childAgents, jobs, call, context).pipe(
            Effect.provideService(FileSystem.FileSystem, fs),
          );
        }
        if (call.capability === workflowJobCapability) {
          return launchJob(sessions, jobs, call, context);
        }
        return Effect.fail(stepError(call, `No adapter is installed for ${call.capability}.`));
      },
      compensate: (call, context) => {
        if (call.capability === workflowAgentCapability) {
          return compensateAgent(childAgents, jobs, call, context);
        }
        if (call.capability === workflowJobCapability) {
          return jobs
            .cancel(
              JobAddress.make({
                jobId: workflowJobId(context.activityKey),
                sessionId: context.sessionId,
              }),
            )
            .pipe(
              Effect.asVoid,
              Effect.mapError((error) => stepError(call, Inspectable.toStringUnknown(error.cause))),
            );
        }
        return Effect.void;
      },
    });
  });

export const layerWorkflowCapabilities = (config: WorkflowCapabilitiesConfig) =>
  Layer.merge(
    Layer.effect(WorkflowCapabilityExecutor, makeCapabilityExecutor(config)),
    layerBunWorkflowArtifactStore(config),
  );
