import {
  WorkflowCapability,
  type WorkflowRunId,
  WorkflowSignalAddress,
  type WorkflowRunRequest,
  type WorkflowSignalName,
  type WorkflowStepId,
} from "@cvr/loom-domain";
import {
  workflowArtifactCapability,
  WorkflowArtifactWrite,
  type WorkflowActivityContext,
  WorkflowBudgetExceededError,
  WorkflowCapabilityDeniedError,
  WorkflowDuplicateStepError,
  WorkflowHostCall,
  WorkflowInterpreterVersionMismatchError,
  WorkflowRunError,
  WorkflowSignalNotDeclaredError,
  WorkflowSourceError,
  WorkflowStepCall,
  WorkflowStepExecution,
  workflowJobCapability,
  type WorkflowArtifactReference,
} from "@cvr/loom-runtime";
import { workflowInterpreterVersion } from "@cvr/loom-protocol";
import { Effect, Option, Schema, Semaphore } from "effect";
import { evaluateWorkflowSource, schemaSourceError } from "./workflow-source-vm.js";

export { makeWorkflowInterpreterHost } from "./workflow-interpreter-host.js";

export interface WorkflowInterpreterHost<R> {
  readonly workflowRunId: WorkflowRunId;
  readonly activity: (
    stepId: WorkflowStepId,
    execute: (
      context: WorkflowActivityContext,
    ) => Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>,
    compensate: (context: WorkflowActivityContext) => Effect.Effect<void, WorkflowRunError, R>,
  ) => Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>;
  readonly parallel: (
    calls: ReadonlyArray<WorkflowStepCall>,
    execute: (
      call: WorkflowStepCall,
      context: WorkflowActivityContext,
    ) => Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>,
  ) => Effect.Effect<ReadonlyArray<WorkflowStepExecution>, WorkflowRunError, R>;
  readonly execute: (
    call: WorkflowStepCall,
    context: WorkflowActivityContext,
  ) => Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>;
  readonly compensate: (
    call: WorkflowStepCall,
    context: WorkflowActivityContext,
  ) => Effect.Effect<void, WorkflowRunError, R>;
  readonly supports: (capability: WorkflowCapability) => boolean;
  readonly storeArtifact: (
    write: WorkflowArtifactWrite,
    context: WorkflowActivityContext,
  ) => Effect.Effect<WorkflowArtifactReference, WorkflowRunError, R>;
  readonly awaitSignal: (
    name: WorkflowSignalName,
  ) => Effect.Effect<Schema.Json, WorkflowRunError, R>;
  readonly withDurationLimit: (
    milliseconds: number,
    evaluation: Effect.Effect<Schema.Json, WorkflowRunError, R>,
  ) => Effect.Effect<Schema.Json, WorkflowRunError, R>;
}

const decodeHostCall = Schema.decodeUnknownEffect(WorkflowHostCall, {
  onExcessProperty: "error",
});
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));
const textEncoder = new TextEncoder();

const budgetError = (
  budget: WorkflowBudgetExceededError["budget"],
  limit: number,
  actual: number,
): WorkflowBudgetExceededError => new WorkflowBudgetExceededError({ budget, limit, actual });

interface WorkflowPassState {
  readonly declaredCapabilities: ReadonlySet<WorkflowCapability>;
  readonly seenStepIds: Set<WorkflowStepId>;
  readonly semaphore: Semaphore.Semaphore;
  readonly usage: { agentRuns: number; tokens: number };
}

const completeStep = <R>(
  request: WorkflowRunRequest,
  host: WorkflowInterpreterHost<R>,
  state: WorkflowPassState,
  call: WorkflowStepCall,
  context: WorkflowActivityContext,
): Effect.Effect<WorkflowStepExecution, WorkflowRunError, R> =>
  Effect.gen(function* () {
    const result = yield* host.execute(call, context);
    const encoded = yield* encodeJson(result.value).pipe(Effect.orDie);
    const bytes = textEncoder.encode(encoded).byteLength;
    if (bytes <= request.budget.maxInlineStepResultBytes) return result;
    if (!state.declaredCapabilities.has(workflowArtifactCapability)) {
      return yield* budgetError(
        "InlineResultBytes",
        request.budget.maxInlineStepResultBytes,
        bytes,
      );
    }
    const value = yield* host.storeArtifact(
      WorkflowArtifactWrite.make({ stepId: call.stepId, value: result.value }),
      context,
    );
    return WorkflowStepExecution.make({ ...result, value });
  });

const validateStep = <R>(
  request: WorkflowRunRequest,
  host: WorkflowInterpreterHost<R>,
  state: WorkflowPassState,
  call: WorkflowStepCall,
): Effect.Effect<void, WorkflowRunError> => {
  if (state.seenStepIds.has(call.stepId)) {
    return new WorkflowDuplicateStepError({ stepId: call.stepId });
  }
  const nextStepCount = state.seenStepIds.size + 1;
  if (nextStepCount > request.budget.maxSteps) {
    return budgetError("Steps", request.budget.maxSteps, nextStepCount);
  }
  if (!state.declaredCapabilities.has(call.capability) || !host.supports(call.capability)) {
    return new WorkflowCapabilityDeniedError({ capability: call.capability });
  }
  state.seenStepIds.add(call.stepId);
  return Effect.void;
};

const recordUsage = (
  request: WorkflowRunRequest,
  state: WorkflowPassState,
  result: WorkflowStepExecution,
): Effect.Effect<void, WorkflowRunError> => {
  state.usage.agentRuns += result.agentRuns;
  state.usage.tokens += result.tokenCount;
  if (state.usage.agentRuns > request.budget.maxAgentRuns) {
    return budgetError("Agents", request.budget.maxAgentRuns, state.usage.agentRuns);
  }
  const exceededTokenBudget = Option.filter(
    request.budget.maxTokens,
    (maximum) => state.usage.tokens > maximum,
  );
  if (Option.isSome(exceededTokenBudget)) {
    return budgetError("Tokens", exceededTokenBudget.value, state.usage.tokens);
  }
  return Effect.void;
};

const makeRunStep = <R>(
  request: WorkflowRunRequest,
  host: WorkflowInterpreterHost<R>,
  state: WorkflowPassState,
) =>
  Effect.fn("WorkflowInterpreter.runStep")(function* (call: WorkflowStepCall) {
    yield* validateStep(request, host, state, call);
    const result = yield* host.activity(
      call.stepId,
      (context) => completeStep(request, host, state, call, context),
      (context) => host.compensate(call, context),
    );
    yield* recordUsage(request, state, result);
    return result;
  });

const makeRunParallel = <R>(
  request: WorkflowRunRequest,
  host: WorkflowInterpreterHost<R>,
  state: WorkflowPassState,
) =>
  Effect.fn("WorkflowInterpreter.runParallel")(function* (calls: ReadonlyArray<WorkflowStepCall>) {
    for (const call of calls) {
      if (call.capability !== workflowJobCapability) {
        return yield* new WorkflowSourceError({
          message: "Promise.all supports only Job Steps.",
        });
      }
    }
    for (const call of calls) yield* validateStep(request, host, state, call);
    const results = yield* host.parallel(calls, (call, context) =>
      Semaphore.withPermit(state.semaphore, completeStep(request, host, state, call, context)),
    );
    for (const result of results) yield* recordUsage(request, state, result);
    return results.map((result) => result.value);
  });

const makeRunHostCall = <R>(
  request: WorkflowRunRequest,
  host: WorkflowInterpreterHost<R>,
  state: WorkflowPassState,
) => {
  const runStep = makeRunStep(request, host, state);
  const runParallel = makeRunParallel(request, host, state);
  const declaredSignals = new Set(request.definition.signals);
  return Effect.fn("WorkflowInterpreter.runHostCall")(function* (received: unknown) {
    const call = yield* decodeHostCall(received).pipe(Effect.mapError(schemaSourceError));
    return yield* WorkflowHostCall.match(call, {
      Parallel: ({ calls }) => runParallel(calls),
      Step: ({ call: stepCall }) => runStep(stepCall).pipe(Effect.map((result) => result.value)),
      Signal: ({ name }) => {
        if (declaredSignals.has(name)) return host.awaitSignal(name);
        return new WorkflowSignalNotDeclaredError({
          address: WorkflowSignalAddress.make({
            sessionId: request.sessionId,
            workflowRunId: host.workflowRunId,
            name,
          }),
        });
      },
    });
  });
};

const evaluatePass = <R>(
  request: WorkflowRunRequest,
  host: WorkflowInterpreterHost<R>,
): Effect.Effect<Schema.Json, WorkflowRunError, R> =>
  Effect.gen(function* () {
    const state: WorkflowPassState = {
      declaredCapabilities: new Set(request.definition.capabilities),
      seenStepIds: new Set(),
      semaphore: yield* Semaphore.make(request.budget.maxParallelism),
      usage: { agentRuns: 0, tokens: 0 },
    };
    const runHostCall = makeRunHostCall(request, host, state);
    const evaluation = evaluateWorkflowSource(request, runHostCall);
    const evaluated = Option.match(request.budget.maxDurationMillis, {
      onNone: () => evaluation,
      onSome: (milliseconds) => host.withDurationLimit(milliseconds, evaluation),
    });
    return yield* evaluated;
  });

export const interpretWorkflow = <R>(
  request: WorkflowRunRequest,
  host: WorkflowInterpreterHost<R>,
): Effect.Effect<Schema.Json, WorkflowRunError, R> => {
  if (request.definition.interpreterVersion !== workflowInterpreterVersion) {
    return new WorkflowInterpreterVersionMismatchError({
      supported: workflowInterpreterVersion,
      received: request.definition.interpreterVersion,
    });
  }
  return evaluatePass(request, host);
};
