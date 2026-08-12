import {
  blockGoal,
  completeGoal,
  GoalState,
  GoalTransitionError,
  goalPluginId,
  goalStateKey,
  JobId,
  PluginStateScope,
  type SessionId,
  WorkflowBudget,
  WorkflowCapability,
  WorkflowDefinition,
  WorkflowKey,
  WorkflowName,
  WorkflowRunRequest,
  WorkflowRunId,
  WorkflowSignalName,
  WorkflowVersion,
  type WorkspaceRoot,
} from "@cvr/loom-domain";
import { LoomClient, makePluginState, type LoomClientShape } from "@cvr/loom-client";
import {
  PluginStateRevision,
  PluginStateRevisionConflictError,
  ReadJobOutputRequest,
  StartJobRequest,
  WaitForJobRequest,
  WorkflowCompensationDecision,
  workflowInterpreterVersion,
} from "@cvr/loom-protocol";
import { Effect, Option, Schema } from "effect";
import { layerBunLoomClient } from "./bun-loom-client.js";

type JobReference = string | { readonly jobId: string };
type WorkflowReference = string | { readonly workflowRunId: string };

interface KernelRuntimeConfig {
  readonly sessionId: SessionId;
  readonly workspaceRoot: WorkspaceRoot;
  readonly nextJobId: () => JobId;
}

const runClient = <A, E>(
  config: KernelRuntimeConfig,
  operation: (client: LoomClientShape) => Effect.Effect<A, E>,
) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.flatMap(LoomClient, operation).pipe(
        Effect.provide(
          layerBunLoomClient({
            workspaceRoot: config.workspaceRoot,
            socketPath: `${config.workspaceRoot}/.loom/daemon.sock`,
            connectionTimeout: "5 seconds",
            requestTimeout: "10 seconds",
          }),
        ),
      ),
    ),
  );

const jobIdFrom = (reference: JobReference) => {
  if (typeof reference === "string") return JobId.make(reference);
  return JobId.make(reference.jobId);
};

const makeJobControls = (config: KernelRuntimeConfig) => ({
  inspect: (job: JobReference) =>
    runClient(config, (client) =>
      client.inspectJob({ sessionId: config.sessionId, jobId: jobIdFrom(job) }),
    ),
  output: (job: JobReference, stream: "stdout" | "stderr" = "stdout", sequence = 0) =>
    runClient(config, (client) =>
      client.readJobOutput(
        ReadJobOutputRequest.make({
          sessionId: config.sessionId,
          jobId: jobIdFrom(job),
          stream,
          sequence,
        }),
      ),
    ),
  wait: (job: JobReference, foregroundLeaseMillis = 300_000) =>
    runClient(config, (client) =>
      client.awaitJob(
        WaitForJobRequest.make({
          sessionId: config.sessionId,
          jobId: jobIdFrom(job),
          foregroundLeaseMillis,
        }),
      ),
    ),
  cancel: (job: JobReference) =>
    runClient(config, (client) =>
      client.cancelJob({ sessionId: config.sessionId, jobId: jobIdFrom(job) }),
    ),
  detach: (job: JobReference) =>
    runClient(config, (client) =>
      client.detachJob({ sessionId: config.sessionId, jobId: jobIdFrom(job) }),
    ),
});

const run = (
  config: KernelRuntimeConfig,
  command: string,
  options: { readonly foregroundLeaseMillis?: number; readonly attached?: boolean } = {},
) =>
  runClient(config, (client) =>
    client.startJob(
      StartJobRequest.make({
        sessionId: config.sessionId,
        jobId: config.nextJobId(),
        command,
        foregroundLeaseMillis: options.foregroundLeaseMillis ?? 300_000,
        attached: options.attached ?? true,
      }),
    ),
  );

const workflowAddress = (config: KernelRuntimeConfig, workflow: WorkflowReference) => {
  if (typeof workflow === "string") {
    return { sessionId: config.sessionId, workflowRunId: WorkflowRunId.make(workflow) };
  }
  return {
    sessionId: config.sessionId,
    workflowRunId: WorkflowRunId.make(workflow.workflowRunId),
  };
};

interface StartWorkflowInput {
  readonly name: string;
  readonly version: string;
  readonly key: string;
  readonly source: string;
  readonly input: Schema.Json;
  readonly capabilities?: ReadonlyArray<string>;
  readonly signals?: ReadonlyArray<string>;
  readonly budget?: {
    readonly maxSteps?: number;
    readonly maxAgentRuns?: number;
    readonly maxParallelism?: number;
    readonly maxInlineStepResultBytes?: number;
    readonly maxTokens?: number;
    readonly maxDurationMillis?: number;
  };
}

const startWorkflow = (config: KernelRuntimeConfig, input: StartWorkflowInput) =>
  runClient(config, (client) =>
    Effect.gen(function* () {
      const budget = yield* Schema.decodeUnknownEffect(WorkflowBudget)(input.budget ?? {});
      return yield* client.startWorkflow(
        WorkflowRunRequest.make({
          sessionId: config.sessionId,
          key: WorkflowKey.make(input.key),
          definition: WorkflowDefinition.make({
            name: WorkflowName.make(input.name),
            version: WorkflowVersion.make(input.version),
            interpreterVersion: workflowInterpreterVersion,
            source: input.source,
            capabilities: (input.capabilities ?? []).map((name) => WorkflowCapability.make(name)),
            signals: (input.signals ?? []).map((name) => WorkflowSignalName.make(name)),
          }),
          input: input.input,
          budget,
        }),
      );
    }),
  );

const makeWorkflowControls = (config: KernelRuntimeConfig) => ({
  start: (input: StartWorkflowInput) => startWorkflow(config, input),
  inspect: (workflow: WorkflowReference) =>
    runClient(config, (client) => client.inspectWorkflow(workflowAddress(config, workflow))),
  signal: (workflow: WorkflowReference, name: string, value: Schema.Json) =>
    runClient(config, (client) =>
      client.signalWorkflow({
        address: {
          ...workflowAddress(config, workflow),
          name: WorkflowSignalName.make(name),
        },
        value,
      }),
    ),
  interrupt: (workflow: WorkflowReference) =>
    runClient(config, (client) => client.interruptWorkflow(workflowAddress(config, workflow))),
  compensate: (workflow: WorkflowReference, decision: "Retry" | "Stop") =>
    runClient(config, (client) =>
      Schema.decodeUnknownEffect(WorkflowCompensationDecision)(decision).pipe(
        Effect.flatMap((decoded) =>
          client.decideWorkflowCompensation({
            address: workflowAddress(config, workflow),
            decision: decoded,
          }),
        ),
      ),
    ),
});

const updateGoal = (
  config: KernelRuntimeConfig,
  transition: (
    state: GoalState,
    revision: PluginStateRevision,
  ) => Effect.Effect<GoalState, GoalTransitionError>,
) =>
  runClient(config, (client) => {
    const state = makePluginState(
      client,
      goalPluginId,
      PluginStateScope.cases.Session.make({ sessionId: config.sessionId }),
      GoalState,
    );
    return Effect.gen(function* () {
      const current = yield* state.read(goalStateKey);
      if (Option.isNone(current)) {
        return yield* new GoalTransitionError({
          reason: "GoalMissing",
          message: "No Goal is available.",
        });
      }
      const revision = PluginStateRevision.make(current.value.revision + 1);
      const next = yield* transition(current.value.value, revision);
      yield* state.write(goalStateKey, next, Option.some(current.value.revision));
      return next;
    }).pipe(
      Effect.retry({
        times: 3,
        while: (error) => error instanceof PluginStateRevisionConflictError,
      }),
    );
  });

const makeGoalControls = (config: KernelRuntimeConfig) => ({
  complete: () => updateGoal(config, completeGoal),
  block: (reason: string) =>
    updateGoal(config, (state, revision) => blockGoal(state, revision, reason)),
});

export const makeKernelRuntimeControls = (config: KernelRuntimeConfig) => ({
  run: (command: string, options: Parameters<typeof run>[2] = {}) => run(config, command, options),
  jobs: makeJobControls(config),
  workflows: makeWorkflowControls(config),
  goal: makeGoalControls(config),
});
