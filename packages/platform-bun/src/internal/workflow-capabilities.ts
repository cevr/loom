import {
  JobAddress,
  JobRequest,
  workflowArtifactId,
  workflowJobId,
  type WorkspaceRoot,
} from "@cvr/loom-domain";
import {
  JobRuntime,
  WorkflowAgentHandle,
  WorkflowAgentInput,
  WorkflowArtifactReference,
  WorkflowArtifactStore,
  WorkflowCapabilityExecutor,
  WorkflowChildAgentStore,
  WorkflowJobHandle,
  WorkflowJobInput,
  WorkflowStepExecution,
  supportsBuiltInWorkflowCapability,
  workflowAgentCapability,
  workflowArtifactCapability,
  workflowJobCapability,
  type WorkflowActivityContext,
  type WorkflowArtifactStoreShape,
  type WorkflowChildAgentStoreShape,
  type WorkflowStepCall,
} from "@cvr/loom-runtime";
import { WorkflowStepError } from "@cvr/loom-protocol";
import { Effect, FileSystem, Inspectable, Layer, Schema } from "effect";

const decodeAgentInput = Schema.decodeUnknownEffect(WorkflowAgentInput);
const decodeJobInput = Schema.decodeUnknownEffect(WorkflowJobInput);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));

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
  store: WorkflowChildAgentStoreShape,
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
) {
  const input = yield* decodeInput(call, decodeAgentInput);
  const agent = yield* store
    .claim(context, input.prompt)
    .pipe(Effect.mapError((error) => stepError(call, error.message)));
  return WorkflowStepExecution.make({
    value: WorkflowAgentHandle.make({ agentId: agent.agentId }),
    tokenCount: 0,
    agentRuns: 1,
  });
});

const launchJob = Effect.fn("WorkflowCapabilities.launchJob")(function* (
  jobs: JobRuntime["Service"],
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
) {
  const input = yield* decodeInput(call, decodeJobInput);
  const jobId = workflowJobId(context.activityKey);
  yield* jobs
    .start(
      JobRequest.make({
        jobId,
        sessionId: context.sessionId,
        command: input.command,
        attached: true,
      }),
    )
    .pipe(Effect.mapError((error) => stepError(call, Inspectable.toStringUnknown(error.cause))));
  return WorkflowStepExecution.make({
    value: WorkflowJobHandle.make({ jobId }),
    tokenCount: 0,
    agentRuns: 0,
  });
});

const makeArtifactStore = (
  workspaceRoot: WorkspaceRoot,
): Effect.Effect<WorkflowArtifactStoreShape, never, FileSystem.FileSystem> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return WorkflowArtifactStore.of({
      store: (write, context) => {
        const artifactId = workflowArtifactId(context.activityKey);
        const path = `${workspaceRoot}/.loom/artifacts/${encodeURIComponent(artifactId)}.json`;
        return fs.makeDirectory(`${workspaceRoot}/.loom/artifacts`, { recursive: true }).pipe(
          Effect.andThen(encodeJson(write.value)),
          Effect.flatMap((value) => fs.writeFileString(path, value)),
          Effect.as(WorkflowArtifactReference.make({ artifactId })),
          Effect.mapError(
            (cause) =>
              new WorkflowStepError({
                stepId: write.stepId,
                capability: workflowArtifactCapability,
                message: Inspectable.toStringUnknown(cause),
              }),
          ),
        );
      },
    });
  });

export interface WorkflowCapabilitiesConfig {
  readonly workspaceRoot: WorkspaceRoot;
}

export const layerWorkflowCapabilities = (config: WorkflowCapabilitiesConfig) =>
  Layer.merge(
    Layer.effect(
      WorkflowCapabilityExecutor,
      Effect.gen(function* () {
        const childAgents = yield* WorkflowChildAgentStore;
        const jobs = yield* JobRuntime;
        return WorkflowCapabilityExecutor.of({
          supports: supportsBuiltInWorkflowCapability,
          execute: (call, context) => {
            if (call.capability === workflowAgentCapability) {
              return launchAgent(childAgents, call, context);
            }
            if (call.capability === workflowJobCapability) {
              return launchJob(jobs, call, context);
            }
            return Effect.fail(stepError(call, `No adapter is installed for ${call.capability}.`));
          },
          compensate: (call, context) => {
            if (call.capability === workflowAgentCapability) {
              return childAgents
                .stop(context.activityKey)
                .pipe(Effect.mapError((error) => stepError(call, error.message)));
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
                  Effect.mapError((error) =>
                    stepError(call, Inspectable.toStringUnknown(error.cause)),
                  ),
                );
            }
            return Effect.void;
          },
        });
      }),
    ),
    Layer.effect(WorkflowArtifactStore, makeArtifactStore(config.workspaceRoot)),
  );
