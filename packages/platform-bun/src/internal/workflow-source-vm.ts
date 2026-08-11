/* oxlint-disable effect/noGlobals, effect/noNodeBuiltinImport -- This adapter owns the unmatched VM APIs. */
import type { WorkflowRunRequest } from "@cvr/loom-domain";
import {
  describeWorkflowSourceError,
  WorkflowBudgetExceededError,
  type WorkflowRunError,
  WorkflowSourceError,
} from "@cvr/loom-runtime";
import { Data, Deferred, Effect, Option, Predicate, Schema } from "effect";
import * as NodeVm from "node:vm";
import {
  makeWorkflowBridge,
  type WorkflowBridge,
  type WorkflowBridgeRequest,
  workflowSourceError,
} from "./workflow-bridge.js";

const decodeResult = Schema.decodeUnknownEffect(Schema.Json);

export const schemaSourceError = (error: Schema.SchemaError) =>
  new WorkflowSourceError({ message: describeWorkflowSourceError(error.message) });

type EvaluationEvent = Data.TaggedEnum<{
  Source: { readonly value: unknown };
  HostCall: { readonly hostRequest: WorkflowBridgeRequest };
}>;
const EvaluationEvent = Data.taggedEnum<EvaluationEvent>();

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

const makeContext = (input: WorkflowRunRequest["input"], bridge: WorkflowBridge): NodeVm.Context =>
  NodeVm.createContext(
    {
      input,
      signal: Object.freeze({
        wait: (name: unknown) => bridge.run({ _tag: "Signal", name }),
      }),
      step: Object.freeze({ run: bridge.runStep }),
      Promise: Object.freeze({ all: bridge.promiseAll }),
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

const executeScript = (
  request: WorkflowRunRequest,
  bridge: WorkflowBridge,
): Effect.Effect<PromiseLike<unknown>, WorkflowRunError> =>
  Effect.gen(function* () {
    const source = `(async () => {\n"use strict"\n${request.definition.source}\n})()`;
    const raw = yield* Effect.try({
      try: () => {
        const script = new NodeVm.Script(source, {
          filename: `${request.definition.name}@${request.definition.version}.workflow.js`,
        });
        return Option.match(request.budget.maxDurationMillis, {
          onNone: () => script.runInContext(makeContext(request.input, bridge)),
          onSome: (milliseconds) =>
            script.runInContext(makeContext(request.input, bridge), { timeout: milliseconds }),
        });
      },
      catch: (cause) => {
        if (
          Option.isSome(request.budget.maxDurationMillis) &&
          Predicate.hasProperty(cause, "code") &&
          cause.code === "ERR_SCRIPT_EXECUTION_TIMEOUT"
        ) {
          const milliseconds = request.budget.maxDurationMillis.value;
          return new WorkflowBudgetExceededError({
            budget: "Duration",
            limit: milliseconds,
            actual: milliseconds,
          });
        }
        return workflowSourceError(cause);
      },
    });
    if (Predicate.isPromiseLike(raw)) return raw;
    return yield* new WorkflowSourceError({
      message: describeWorkflowSourceError("Workflow source did not return a Promise."),
    });
  });

const runEventLoop = <R>(
  raw: PromiseLike<unknown>,
  bridge: WorkflowBridge,
  runHostCall: (received: unknown) => Effect.Effect<Schema.Json, WorkflowRunError, R>,
): Effect.Effect<Schema.Json, WorkflowRunError, R> =>
  Effect.gen(function* () {
    const completed = yield* Deferred.make<unknown, WorkflowRunError>();
    raw.then(
      (value) => Deferred.doneUnsafe(completed, Effect.succeed(value)),
      (cause) => Deferred.doneUnsafe(completed, Effect.fail(bridge.sourceError(cause))),
    );
    while (true) {
      const event = yield* Effect.raceFirst(
        Deferred.await(completed).pipe(Effect.map((value) => EvaluationEvent.Source({ value }))),
        bridge.next.pipe(Effect.map((hostRequest) => EvaluationEvent.HostCall({ hostRequest }))),
      );
      if (EvaluationEvent.$is("Source")(event)) {
        return yield* decodeResult(event.value).pipe(Effect.mapError(schemaSourceError));
      }
      const exit = yield* runHostCall(event.hostRequest.received).pipe(Effect.exit);
      yield* bridge.settle(event.hostRequest, exit);
    }
  });

export const evaluateWorkflowSource = <R>(
  request: WorkflowRunRequest,
  runHostCall: (received: unknown) => Effect.Effect<Schema.Json, WorkflowRunError, R>,
): Effect.Effect<Schema.Json, WorkflowRunError, R> =>
  Effect.gen(function* () {
    const bridge = yield* makeWorkflowBridge;
    return yield* executeScript(request, bridge).pipe(
      Effect.flatMap((raw) => runEventLoop(raw, bridge, runHostCall)),
      Effect.ensuring(bridge.deactivate),
    );
  });
