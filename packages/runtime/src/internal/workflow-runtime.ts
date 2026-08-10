import { WorkflowRunId, type WorkflowRunRequest } from "@cvr/loom-domain";
import {
  type ExecuteWorkflowError,
  type SignalWorkflowError,
  type StartWorkflowError,
  type WorkflowIdentityConflictError,
  type WorkflowRunAcceptanceError,
  WorkflowSignalNotDeclaredError,
  type SignalWorkflowRequest,
  type WorkflowRunError,
} from "@cvr/loom-protocol";
import { Context, Effect, Layer, Schema, Stream, type Duration } from "effect";
import { WorkflowEngine } from "effect/unstable/workflow";
import type { PeekResult } from "effect-encore";
import { LoomDynamicWorkflow, loomWorkflowSignal } from "./loom-dynamic-workflow.js";
import { WorkflowRunAcceptance } from "./workflow-run-acceptance.js";
import { WorkflowSignalDeclarations } from "./workflow-signal-declarations.js";

export type WorkflowRuntimeAcceptanceError = StartWorkflowError;
export type WorkflowRuntimeError = ExecuteWorkflowError;
export type WorkflowRuntimeReadError = WorkflowIdentityConflictError | WorkflowRunAcceptanceError;
export type WorkflowRuntimeSignalError = SignalWorkflowError;
export type WorkflowRuntimeState = PeekResult<Schema.Json, WorkflowRunError>;

export interface WorkflowRuntimeShape {
  readonly execute: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<Schema.Json, WorkflowRuntimeError>;
  readonly send: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<WorkflowRunId, WorkflowRuntimeAcceptanceError>;
  readonly signal: (
    request: SignalWorkflowRequest,
  ) => Effect.Effect<void, WorkflowRuntimeSignalError>;
  readonly peek: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<WorkflowRuntimeState, WorkflowRuntimeReadError>;
  readonly watch: (
    request: WorkflowRunRequest,
    options?: { readonly interval?: Duration.Input },
  ) => Stream.Stream<WorkflowRuntimeState, WorkflowRuntimeReadError>;
  readonly wait: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<WorkflowRuntimeState, WorkflowRuntimeReadError>;
  readonly interrupt: (workflowRunId: WorkflowRunId) => Effect.Effect<void>;
  readonly resume: (workflowRunId: WorkflowRunId) => Effect.Effect<void>;
}

export class WorkflowRuntime extends Context.Service<WorkflowRuntime, WorkflowRuntimeShape>()(
  "@cvr/loom-runtime/WorkflowRuntime",
) {}

interface AcceptedWorkflow {
  readonly request: WorkflowRunRequest;
  readonly workflowRunId: WorkflowRunId;
}

type AcceptWorkflow = (
  request: WorkflowRunRequest,
) => Effect.Effect<AcceptedWorkflow, WorkflowRuntimeReadError>;

const makeAcceptWorkflow = (acceptance: WorkflowRunAcceptance["Service"]): AcceptWorkflow =>
  Effect.fn("WorkflowRuntime.accept")(function* (received: WorkflowRunRequest) {
    const { request } = yield* acceptance.accept(received);
    const executionId = yield* LoomDynamicWorkflow.executionId(request);
    const workflowRunId = WorkflowRunId.make(executionId);
    return { request, workflowRunId };
  });

const makeSignalWorkflow =
  (
    declarations: WorkflowSignalDeclarations["Service"],
    engine: WorkflowEngine.WorkflowEngine["Service"],
  ): WorkflowRuntimeShape["signal"] =>
  ({ address, value }) =>
    declarations.contains(address.workflowRunId, address.name).pipe(
      Effect.flatMap((declared) => {
        if (declared) {
          const signal = loomWorkflowSignal(address.name);
          return signal.succeedAt(address.workflowRunId, value);
        }
        return new WorkflowSignalNotDeclaredError({ address });
      }),
      Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
    );

const makeRuntime = (
  accept: AcceptWorkflow,
  client: Effect.Success<typeof LoomDynamicWorkflow.Context>,
  engine: WorkflowEngine.WorkflowEngine["Service"],
  declarations: WorkflowSignalDeclarations["Service"],
): WorkflowRuntimeShape => {
  const provideClient = Effect.provideService(LoomDynamicWorkflow.Context, client);
  const provideEngine = Effect.provideService(WorkflowEngine.WorkflowEngine, engine);
  const declare = (accepted: AcceptedWorkflow) =>
    declarations.declare(accepted.workflowRunId, accepted.request.definition.signals);
  return {
    execute: (request) =>
      accept(request).pipe(
        Effect.tap(declare),
        Effect.flatMap(({ request: accepted }) => LoomDynamicWorkflow.execute(accepted)),
        provideClient,
      ),
    send: (request) =>
      accept(request).pipe(
        Effect.tap(declare),
        Effect.flatMap(({ request: accepted, workflowRunId }) =>
          LoomDynamicWorkflow.send(accepted).pipe(Effect.as(workflowRunId)),
        ),
        provideClient,
      ),
    signal: makeSignalWorkflow(declarations, engine),
    peek: (request) =>
      accept(request).pipe(
        Effect.flatMap(({ request: accepted }) => LoomDynamicWorkflow.peek(accepted)),
        provideEngine,
      ),
    watch: (request, options) =>
      Stream.unwrap(
        accept(request).pipe(
          Effect.map(({ request: accepted }) =>
            LoomDynamicWorkflow.watch(accepted, options).pipe(
              Stream.provideService(WorkflowEngine.WorkflowEngine, engine),
            ),
          ),
        ),
      ),
    wait: (request) =>
      accept(request).pipe(
        Effect.flatMap(({ request: accepted }) => LoomDynamicWorkflow.waitFor(accepted)),
        provideEngine,
      ),
    interrupt: (workflowRunId) => LoomDynamicWorkflow.interrupt(workflowRunId).pipe(provideEngine),
    resume: (workflowRunId) => LoomDynamicWorkflow.resume(workflowRunId).pipe(provideEngine),
  };
};

export const makeWorkflowRuntime = Effect.gen(function* () {
  const acceptance = yield* WorkflowRunAcceptance;
  const client = yield* LoomDynamicWorkflow.Context;
  const engine = yield* WorkflowEngine.WorkflowEngine;
  const declarations = yield* WorkflowSignalDeclarations;
  return WorkflowRuntime.of(
    makeRuntime(makeAcceptWorkflow(acceptance), client, engine, declarations),
  );
});

export const layerWorkflowRuntime = Layer.effect(WorkflowRuntime, makeWorkflowRuntime);
