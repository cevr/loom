/* oxlint-disable effect/noGlobals, effect/noNodeBuiltinImport -- This adapter owns the unmatched VM APIs. */
import { WorkflowCapability, type WorkflowRunRequest, type WorkflowStepId } from "@cvr/loom-domain";
import {
  WorkflowArtifactWrite,
  WorkflowBudgetExceededError,
  WorkflowCapabilityDeniedError,
  WorkflowDuplicateStepError,
  WorkflowInterpreterVersionMismatchError,
  WorkflowRunError,
  WorkflowSourceError,
  WorkflowStepCall,
  WorkflowStepExecution,
  type WorkflowArtifactReference,
} from "@cvr/loom-runtime";
import { workflowInterpreterVersion } from "@cvr/loom-protocol";
import { Effect, Option, Predicate, Schema, Scope, Semaphore } from "effect";
import * as NodeVm from "node:vm";
import { makeWorkflowBridge, type WorkflowBridge, workflowSourceError } from "./workflow-bridge.js";

export interface WorkflowInterpreterHost<R> {
  readonly activity: (
    stepId: WorkflowStepId,
    execute: Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>,
  ) => Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>;
  readonly execute: (
    call: WorkflowStepCall,
  ) => Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>;
  readonly supports: (capability: WorkflowCapability) => boolean;
  readonly storeArtifact: (
    write: WorkflowArtifactWrite,
  ) => Effect.Effect<WorkflowArtifactReference, WorkflowRunError, R>;
  readonly withDurationLimit: (
    milliseconds: number,
    evaluation: Effect.Effect<Schema.Json, WorkflowRunError, R>,
  ) => Effect.Effect<Schema.Json, WorkflowRunError, R>;
}

const decodeCall = Schema.decodeUnknownEffect(WorkflowStepCall);
const decodeResult = Schema.decodeUnknownEffect(Schema.Json);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));
const textEncoder = new TextEncoder();

/* oxlint-disable effect/noNullish -- The VM sandbox requires a null prototype and explicit undefined globals. */
const deterministicMath = (): object => {
  const target = Object.create(null);
  for (const key of Reflect.ownKeys(Math)) {
    if (key === "random") continue;
    const descriptor = Reflect.getOwnPropertyDescriptor(Math, key);
    if (descriptor !== undefined) Reflect.defineProperty(target, key, descriptor);
  }
  return Object.freeze(target);
};

const makeContext = (
  input: WorkflowRunRequest["input"],
  run: (call: unknown) => Promise<unknown>,
): NodeVm.Context =>
  NodeVm.createContext(
    {
      input,
      step: Object.freeze({ run }),
      Bun: undefined,
      Date: undefined,
      fetch: undefined,
      Math: deterministicMath(),
      module: undefined,
      process: undefined,
      require: undefined,
    },
    {
      name: "Loom Workflow",
      codeGeneration: { strings: false, wasm: false },
    },
  );
/* oxlint-enable effect/noNullish */

const evaluateSource = (
  request: WorkflowRunRequest,
  bridge: WorkflowBridge,
): Effect.Effect<Schema.Json, WorkflowRunError> =>
  Effect.gen(function* () {
    const context = makeContext(request.input, bridge.run);
    const source = `(async () => {\n"use strict"\n${request.definition.source}\n})()`;
    const raw = yield* Effect.try({
      try: () => {
        const script = new NodeVm.Script(source, {
          filename: `${request.definition.name}@${request.definition.version}.workflow.js`,
        });
        return Option.match(request.budget.maxDurationMillis, {
          onNone: () => script.runInContext(context),
          onSome: (milliseconds) => script.runInContext(context, { timeout: milliseconds }),
        });
      },
      catch: (cause) =>
        Option.match(request.budget.maxDurationMillis, {
          onNone: () => workflowSourceError(cause),
          onSome: (milliseconds) => {
            if (
              Predicate.hasProperty(cause, "code") &&
              cause.code === "ERR_SCRIPT_EXECUTION_TIMEOUT"
            ) {
              return budgetError("Duration", milliseconds, milliseconds);
            }
            return workflowSourceError(cause);
          },
        }),
    });
    if (!Predicate.isPromiseLike(raw)) {
      return yield* new WorkflowSourceError({
        message: "Workflow source did not return a Promise.",
      });
    }
    const value = yield* Effect.tryPromise({
      try: () => raw,
      catch: bridge.sourceError,
    });
    return yield* decodeResult(value).pipe(Effect.mapError(workflowSourceError));
  });

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
): Effect.Effect<WorkflowStepExecution, WorkflowRunError, R> =>
  Effect.gen(function* () {
    const result = yield* host.execute(call);
    const encoded = yield* encodeJson(result.value).pipe(Effect.orDie);
    const bytes = textEncoder.encode(encoded).byteLength;
    if (bytes <= request.budget.maxInlineStepResultBytes) return result;
    if (!state.declaredCapabilities.has(WorkflowCapability.make("artifact"))) {
      return yield* budgetError(
        "InlineResultBytes",
        request.budget.maxInlineStepResultBytes,
        bytes,
      );
    }
    const value = yield* host.storeArtifact(
      WorkflowArtifactWrite.make({ stepId: call.stepId, value: result.value }),
    );
    return WorkflowStepExecution.make({ ...result, value });
  });

const makeRunStep = <R>(
  request: WorkflowRunRequest,
  host: WorkflowInterpreterHost<R>,
  state: WorkflowPassState,
) =>
  Effect.fn("WorkflowInterpreter.runStep")(function* (received: unknown) {
    const call = yield* decodeCall(received).pipe(Effect.mapError(workflowSourceError));
    if (state.seenStepIds.has(call.stepId)) {
      return yield* new WorkflowDuplicateStepError({ stepId: call.stepId });
    }
    state.seenStepIds.add(call.stepId);
    if (state.seenStepIds.size > request.budget.maxSteps) {
      return yield* budgetError("Steps", request.budget.maxSteps, state.seenStepIds.size);
    }
    if (!state.declaredCapabilities.has(call.capability) || !host.supports(call.capability)) {
      return yield* new WorkflowCapabilityDeniedError({ capability: call.capability });
    }

    const result = yield* Semaphore.withPermit(
      state.semaphore,
      host.activity(call.stepId, completeStep(request, host, state, call)),
    );
    state.usage.agentRuns += result.agentRuns;
    state.usage.tokens += result.tokenCount;
    if (state.usage.agentRuns > request.budget.maxAgentRuns) {
      return yield* budgetError("Agents", request.budget.maxAgentRuns, state.usage.agentRuns);
    }
    const exceededTokenBudget = Option.filter(
      request.budget.maxTokens,
      (maximum) => state.usage.tokens > maximum,
    );
    if (Option.isSome(exceededTokenBudget)) {
      return yield* budgetError("Tokens", exceededTokenBudget.value, state.usage.tokens);
    }
    return result;
  });

const evaluatePass = <R>(
  request: WorkflowRunRequest,
  host: WorkflowInterpreterHost<R>,
): Effect.Effect<Schema.Json, WorkflowRunError, R | Scope.Scope> =>
  Effect.gen(function* () {
    const state: WorkflowPassState = {
      declaredCapabilities: new Set(request.definition.capabilities),
      seenStepIds: new Set(),
      semaphore: yield* Semaphore.make(request.budget.maxParallelism),
      usage: { agentRuns: 0, tokens: 0 },
    };
    const bridge = yield* makeWorkflowBridge(makeRunStep(request, host, state));
    const evaluation = Effect.raceFirst(evaluateSource(request, bridge), bridge.fatal);
    const evaluated = Option.match(request.budget.maxDurationMillis, {
      onNone: () => evaluation,
      onSome: (milliseconds) => host.withDurationLimit(milliseconds, evaluation),
    });
    return yield* evaluated.pipe(Effect.ensuring(bridge.deactivate));
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
  return Effect.scoped(evaluatePass(request, host));
};
