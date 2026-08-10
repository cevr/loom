import { type WorkflowRunAddress, WorkflowRunId, type WorkflowRunRequest } from "@cvr/loom-domain";
import {
  type ExecuteWorkflowError,
  type DecideWorkflowCompensationError,
  type DecideWorkflowCompensationRequest,
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
import { Context, Effect, Layer, Schema, Stream, type Duration } from "effect";
import { WorkflowEngine } from "effect/unstable/workflow";
import type { PeekResult } from "effect-encore";
import { LoomDynamicWorkflow, loomWorkflowSignal } from "./loom-dynamic-workflow.js";
import { WorkflowRunAcceptance } from "./workflow-run-acceptance.js";
import { WorkflowSignalDeclarations } from "./workflow-signal-declarations.js";
import { toWorkflowRunState, WorkflowRunStatePublisher } from "./workflow-run-state-publisher.js";
import { makeDecideCompensation } from "./workflow-compensation-control.js";

export type WorkflowRuntimeAcceptanceError = StartWorkflowError;
export type WorkflowRuntimeError = ExecuteWorkflowError;
export type WorkflowRuntimeReadError = WorkflowIdentityConflictError | WorkflowRunAcceptanceError;
export type WorkflowRuntimeSignalError = SignalWorkflowError;
export type WorkflowRuntimeState = PeekResult<Schema.Json, WorkflowRunError>;
export type WorkflowRuntimeInspectError = WorkflowRunNotFoundError | WorkflowRunAcceptanceError;
export type WorkflowRuntimeResumeError = WorkflowRuntimeInspectError | WorkflowRunNotSuspendedError;
export type WorkflowRuntimeCompensationError = DecideWorkflowCompensationError;

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
  readonly decideCompensation: (
    request: DecideWorkflowCompensationRequest,
  ) => Effect.Effect<void, WorkflowRuntimeCompensationError>;
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
    acceptance: WorkflowRunAcceptance["Service"],
    declarations: WorkflowSignalDeclarations["Service"],
    engine: WorkflowEngine.WorkflowEngine["Service"],
    publisher: WorkflowRunStatePublisher["Service"],
  ): WorkflowRuntimeShape["signal"] =>
  ({ address, value }) =>
    acceptance.authorize(address).pipe(
      Effect.andThen(declarations.contains(address.workflowRunId, address.name)),
      Effect.flatMap((declared) => {
        if (declared) {
          const signal = loomWorkflowSignal(address.name);
          return signal.succeedAt(address.workflowRunId, value);
        }
        return new WorkflowSignalNotDeclaredError({ address });
      }),
      Effect.tap(() => publisher.watch(address)),
      Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
    );

const makeRequireSuspended = (
  acceptance: WorkflowRunAcceptance["Service"],
  engine: WorkflowEngine.WorkflowEngine["Service"],
) =>
  Effect.fn("WorkflowRuntime.requireSuspended")(function* (address: WorkflowRunAddress) {
    yield* acceptance.authorize(address);
    const state = yield* LoomDynamicWorkflow.peekAt(address.workflowRunId).pipe(
      Effect.map(toWorkflowRunState),
      Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
    );
    if (!WorkflowRunState.guards.Suspended(state)) {
      return yield* new WorkflowRunNotSuspendedError({ address, state });
    }
  });

const makeControlWorkflow = (
  acceptance: WorkflowRunAcceptance["Service"],
  engine: WorkflowEngine.WorkflowEngine["Service"],
  publisher: WorkflowRunStatePublisher["Service"],
) => {
  const provideEngine = Effect.provideService(WorkflowEngine.WorkflowEngine, engine);
  const requireSuspended = makeRequireSuspended(acceptance, engine);
  const inspect = Effect.fn("WorkflowRuntime.inspect")(function* (address: WorkflowRunAddress) {
    yield* acceptance.authorize(address);
    yield* publisher.watch(address);
    return yield* LoomDynamicWorkflow.peekAt(address.workflowRunId).pipe(
      Effect.map(toWorkflowRunState),
      provideEngine,
    );
  });
  return {
    inspect,
    interrupt: (address: WorkflowRunAddress) =>
      acceptance.authorize(address).pipe(
        Effect.andThen(LoomDynamicWorkflow.interrupt(address.workflowRunId)),
        Effect.tap(() => publisher.watch(address)),
        provideEngine,
      ),
    resume: (address: WorkflowRunAddress) =>
      requireSuspended(address).pipe(
        Effect.andThen(LoomDynamicWorkflow.resume(address.workflowRunId)),
        Effect.tap(() => publisher.watch(address)),
        provideEngine,
      ),
    decideCompensation: makeDecideCompensation(acceptance, engine, publisher),
  };
};

const makeDeclareWorkflow =
  (declarations: WorkflowSignalDeclarations["Service"]) => (accepted: AcceptedWorkflow) =>
    declarations.declare(accepted.workflowRunId, accepted.request.definition.signals);

const makeExecuteWorkflow =
  (
    accept: AcceptWorkflow,
    declare: ReturnType<typeof makeDeclareWorkflow>,
    client: Effect.Success<typeof LoomDynamicWorkflow.Context>,
    publisher: WorkflowRunStatePublisher["Service"],
  ): WorkflowRuntimeShape["execute"] =>
  (request) =>
    accept(request).pipe(
      Effect.tap(declare),
      Effect.tap(({ request: accepted }) => LoomDynamicWorkflow.send(accepted)),
      Effect.tap(({ request: accepted, workflowRunId }) =>
        publisher.watch({ sessionId: accepted.sessionId, workflowRunId }),
      ),
      Effect.flatMap(({ request: accepted }) => LoomDynamicWorkflow.execute(accepted)),
      Effect.provideService(LoomDynamicWorkflow.Context, client),
    );

const makeSendWorkflow =
  (
    accept: AcceptWorkflow,
    declare: ReturnType<typeof makeDeclareWorkflow>,
    client: Effect.Success<typeof LoomDynamicWorkflow.Context>,
    publisher: WorkflowRunStatePublisher["Service"],
  ): WorkflowRuntimeShape["send"] =>
  (request) =>
    accept(request).pipe(
      Effect.tap(declare),
      Effect.tap(({ request: accepted }) => LoomDynamicWorkflow.send(accepted)),
      Effect.tap(({ request: accepted, workflowRunId }) =>
        publisher.watch({ sessionId: accepted.sessionId, workflowRunId }),
      ),
      Effect.map(({ workflowRunId }) => workflowRunId),
      Effect.provideService(LoomDynamicWorkflow.Context, client),
    );

const makeRuntime = (
  accept: AcceptWorkflow,
  acceptance: WorkflowRunAcceptance["Service"],
  client: Effect.Success<typeof LoomDynamicWorkflow.Context>,
  engine: WorkflowEngine.WorkflowEngine["Service"],
  declarations: WorkflowSignalDeclarations["Service"],
  publisher: WorkflowRunStatePublisher["Service"],
): WorkflowRuntimeShape => {
  const provideEngine = Effect.provideService(WorkflowEngine.WorkflowEngine, engine);
  const declare = makeDeclareWorkflow(declarations);
  return {
    execute: makeExecuteWorkflow(accept, declare, client, publisher),
    send: makeSendWorkflow(accept, declare, client, publisher),
    signal: makeSignalWorkflow(acceptance, declarations, engine, publisher),
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
    ...makeControlWorkflow(acceptance, engine, publisher),
  };
};

export const makeWorkflowRuntime = Effect.gen(function* () {
  const acceptance = yield* WorkflowRunAcceptance;
  const client = yield* LoomDynamicWorkflow.Context;
  const engine = yield* WorkflowEngine.WorkflowEngine;
  const declarations = yield* WorkflowSignalDeclarations;
  const publisher = yield* WorkflowRunStatePublisher;
  yield* Effect.forEach(yield* acceptance.list, publisher.watch, {
    concurrency: "unbounded",
    discard: true,
  });
  return WorkflowRuntime.of(
    makeRuntime(
      makeAcceptWorkflow(acceptance),
      acceptance,
      client,
      engine,
      declarations,
      publisher,
    ),
  );
});

export const layerWorkflowRuntime = Layer.effect(WorkflowRuntime, makeWorkflowRuntime);
