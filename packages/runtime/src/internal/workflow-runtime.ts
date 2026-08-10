import { type WorkflowRunAddress, WorkflowRunId, type WorkflowRunRequest } from "@cvr/loom-domain";
import {
  type ExecuteWorkflowError,
  type SignalWorkflowError,
  type StartWorkflowError,
  type WorkflowIdentityConflictError,
  type WorkflowRunAcceptanceError,
  WorkflowSignalNotDeclaredError,
  type SignalWorkflowRequest,
  type WorkflowRunError,
  WorkflowRunState,
  type WorkflowRunNotFoundError,
  WorkflowRunNotSuspendedError,
} from "@cvr/loom-protocol";
import { Context, Effect, Inspectable, Layer, Match, Schema, Stream, type Duration } from "effect";
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
export type WorkflowRuntimeInspectError = WorkflowRunNotFoundError | WorkflowRunAcceptanceError;
export type WorkflowRuntimeResumeError = WorkflowRuntimeInspectError | WorkflowRunNotSuspendedError;

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
  readonly inspect: (
    address: WorkflowRunAddress,
  ) => Effect.Effect<WorkflowRunState, WorkflowRuntimeInspectError>;
  readonly interrupt: (
    address: WorkflowRunAddress,
  ) => Effect.Effect<void, WorkflowRuntimeInspectError>;
  readonly resume: (address: WorkflowRunAddress) => Effect.Effect<void, WorkflowRuntimeResumeError>;
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
    const executionId = yield* LoomDynamicWorkflow.executionId(received);
    const workflowRunId = WorkflowRunId.make(executionId);
    const { request } = yield* acceptance.accept(received, workflowRunId);
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

const toWorkflowRunState = (state: WorkflowRuntimeState): WorkflowRunState =>
  Match.value(state).pipe(
    Match.tag("Defect", ({ cause }) =>
      WorkflowRunState.cases.Defect.make({ message: Inspectable.toStringUnknown(cause) }),
    ),
    Match.orElse((result) => result),
  );

const makeControlWorkflow = (
  acceptance: WorkflowRunAcceptance["Service"],
  engine: WorkflowEngine.WorkflowEngine["Service"],
) => {
  const provideEngine = Effect.provideService(WorkflowEngine.WorkflowEngine, engine);
  const inspect = Effect.fn("WorkflowRuntime.inspect")(function* (address: WorkflowRunAddress) {
    yield* acceptance.authorize(address);
    return yield* LoomDynamicWorkflow.peekAt(address.workflowRunId).pipe(
      Effect.map(toWorkflowRunState),
      provideEngine,
    );
  });
  return {
    inspect,
    interrupt: (address: WorkflowRunAddress) =>
      acceptance
        .authorize(address)
        .pipe(Effect.andThen(LoomDynamicWorkflow.interrupt(address.workflowRunId)), provideEngine),
    resume: (address: WorkflowRunAddress) =>
      acceptance.authorize(address).pipe(
        Effect.andThen(LoomDynamicWorkflow.peekAt(address.workflowRunId)),
        Effect.map(toWorkflowRunState),
        Effect.filterOrFail(
          WorkflowRunState.guards.Suspended,
          (state) => new WorkflowRunNotSuspendedError({ address, state }),
        ),
        Effect.andThen(LoomDynamicWorkflow.resume(address.workflowRunId)),
        provideEngine,
      ),
  };
};

const makeDeclareWorkflow =
  (declarations: WorkflowSignalDeclarations["Service"]) => (accepted: AcceptedWorkflow) =>
    declarations.declare(accepted.workflowRunId, accepted.request.definition.signals);

const makeRuntime = (
  accept: AcceptWorkflow,
  acceptance: WorkflowRunAcceptance["Service"],
  client: Effect.Success<typeof LoomDynamicWorkflow.Context>,
  engine: WorkflowEngine.WorkflowEngine["Service"],
  declarations: WorkflowSignalDeclarations["Service"],
): WorkflowRuntimeShape => {
  const provideClient = Effect.provideService(LoomDynamicWorkflow.Context, client);
  const provideEngine = Effect.provideService(WorkflowEngine.WorkflowEngine, engine);
  const declare = makeDeclareWorkflow(declarations);
  const control = makeControlWorkflow(acceptance, engine);
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
    ...control,
  };
};

export const makeWorkflowRuntime = Effect.gen(function* () {
  const acceptance = yield* WorkflowRunAcceptance;
  const client = yield* LoomDynamicWorkflow.Context;
  const engine = yield* WorkflowEngine.WorkflowEngine;
  const declarations = yield* WorkflowSignalDeclarations;
  return WorkflowRuntime.of(
    makeRuntime(makeAcceptWorkflow(acceptance), acceptance, client, engine, declarations),
  );
});

export const layerWorkflowRuntime = Layer.effect(WorkflowRuntime, makeWorkflowRuntime);
