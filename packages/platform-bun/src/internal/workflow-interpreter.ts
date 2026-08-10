/* oxlint-disable effect/noGlobals, effect/noNewPromise, effect/noNodeBuiltinImport -- This adapter owns the unmatched VM and Promise bridge. */
import { WorkflowCapability, type WorkflowRunRequest, type WorkflowStepId } from "@cvr/loom-domain";
import {
  WorkflowArtifactWrite,
  WorkflowBudgetExceededError,
  WorkflowCapabilityDeniedError,
  WorkflowInterpreterVersionMismatchError,
  WorkflowRunError,
  WorkflowSourceError,
  WorkflowStepCall,
  WorkflowStepExecution,
  type WorkflowArtifactReference,
} from "@cvr/loom-runtime";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  FiberSet,
  Inspectable,
  Option,
  Predicate,
  Schema,
  Scope,
  Semaphore,
} from "effect";
import * as NodeVm from "node:vm";

export const workflowInterpreterVersion = 1;

export interface WorkflowInterpreterHost<R> {
  readonly activity: (
    stepId: WorkflowStepId,
    execute: Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>,
  ) => Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>;
  readonly execute: (
    call: WorkflowStepCall,
  ) => Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>;
  readonly storeArtifact: (
    write: WorkflowArtifactWrite,
  ) => Effect.Effect<WorkflowArtifactReference, WorkflowRunError, R>;
  readonly durationLimit: (
    milliseconds: number,
  ) => Effect.Effect<never, WorkflowBudgetExceededError, R>;
}

const decodeCall = Schema.decodeUnknownEffect(WorkflowStepCall);
const decodeResult = Schema.decodeUnknownEffect(Schema.Json);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));
const textEncoder = new TextEncoder();

const sourceError = (cause: unknown): WorkflowSourceError =>
  new WorkflowSourceError({ message: Inspectable.toStringUnknown(cause) });

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
        if (request.budget.maxDurationMillis === null) return script.runInContext(context);
        return script.runInContext(context, { timeout: request.budget.maxDurationMillis });
      },
      catch: (cause) => {
        if (
          request.budget.maxDurationMillis !== null &&
          Predicate.hasProperty(cause, "code") &&
          cause.code === "ERR_SCRIPT_EXECUTION_TIMEOUT"
        ) {
          return budgetError(
            "Duration",
            request.budget.maxDurationMillis,
            request.budget.maxDurationMillis,
          );
        }
        return sourceError(cause);
      },
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
    return yield* decodeResult(value).pipe(Effect.mapError(sourceError));
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
    const call = yield* decodeCall(received).pipe(Effect.mapError(sourceError));
    if (state.seenStepIds.has(call.stepId)) {
      return yield* Effect.die(`Duplicate Workflow Step ID: ${call.stepId}`);
    }
    state.seenStepIds.add(call.stepId);
    if (state.seenStepIds.size > request.budget.maxSteps) {
      return yield* budgetError("Steps", request.budget.maxSteps, state.seenStepIds.size);
    }
    if (!state.declaredCapabilities.has(call.capability)) {
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
    if (request.budget.maxTokens !== null && state.usage.tokens > request.budget.maxTokens) {
      return yield* budgetError("Tokens", request.budget.maxTokens, state.usage.tokens);
    }
    return result;
  });

interface WorkflowBridge {
  readonly fatal: Effect.Effect<never, WorkflowRunError>;
  readonly run: (received: unknown) => Promise<unknown>;
  readonly sourceError: (cause: unknown) => WorkflowRunError;
  readonly deactivate: Effect.Effect<void>;
}

const makeBridge = <R>(
  runStep: (received: unknown) => Effect.Effect<WorkflowStepExecution, WorkflowRunError, R>,
): Effect.Effect<WorkflowBridge, never, R | Scope.Scope> =>
  Effect.gen(function* () {
    const fibers = yield* FiberSet.make<WorkflowStepExecution, WorkflowRunError>();
    const fatal = yield* Deferred.make<never, WorkflowRunError>();
    const runFork = yield* FiberSet.runtime(fibers)<R>();
    const failures = new WeakMap<object, WorkflowRunError>();
    let active = true;
    const run = (received: unknown): Promise<unknown> =>
      new Promise((resolve, reject) => {
        if (!active) return;
        runFork(runStep(received)).addObserver((exit) => {
          if (Exit.isSuccess(exit)) {
            resolve(exit.value.value);
            return;
          }
          const error = Cause.findErrorOption(exit.cause);
          if (
            Option.isSome(error) &&
            !Cause.hasDies(exit.cause) &&
            !Cause.hasInterrupts(exit.cause)
          ) {
            const failure = Object.freeze({});
            failures.set(failure, error.value);
            reject(failure);
            return;
          }
          Deferred.doneUnsafe(fatal, Effect.failCause(exit.cause));
        });
      });
    return {
      fatal: Deferred.await(fatal),
      run,
      sourceError: (cause) => {
        if (Predicate.isObject(cause)) {
          const error = failures.get(cause);
          if (error !== undefined) return error;
        }
        return sourceError(cause);
      },
      deactivate: Effect.sync(() => {
        active = false;
      }),
    };
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
    const bridge = yield* makeBridge(makeRunStep(request, host, state));
    const evaluation = Effect.raceFirst(evaluateSource(request, bridge), bridge.fatal);
    if (request.budget.maxDurationMillis === null) {
      return yield* evaluation.pipe(Effect.ensuring(bridge.deactivate));
    }
    return yield* Effect.raceFirst(
      evaluation,
      host.durationLimit(request.budget.maxDurationMillis),
    ).pipe(Effect.ensuring(bridge.deactivate));
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
