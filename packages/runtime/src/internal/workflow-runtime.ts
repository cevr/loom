import {
  type AcceptedWorkflowRun,
  type WorkflowRunAddress,
  WorkflowRunExecution,
  type WorkflowRunId,
  type WorkflowRunRequest,
} from "@cvr/loom-domain";
import {
  type ExecuteWorkflowError,
  type DecideWorkflowCompensationError,
  type DecideWorkflowCompensationRequest,
  type SignalWorkflowError,
  type StartWorkflowError,
  type WorkflowRunAcceptanceError,
  WorkflowSignalNotDeclaredError,
  type SignalWorkflowRequest,
  type WorkflowRunError,
  WorkflowRunState,
  type WorkflowRunNotFoundError,
} from "@cvr/loom-protocol";
import { Context, Effect, Layer, Schema, Stream, type Duration } from "effect";
import { WorkflowEngine } from "effect/unstable/workflow";
import { Client, type PeekResult } from "effect-encore";
import { LoomDynamicWorkflow, loomWorkflowSignal } from "./loom-dynamic-workflow.js";
import { WorkflowRunAcceptance } from "./workflow-run-acceptance.js";
import { WorkflowSignalDeclarations } from "./workflow-signal-declarations.js";
import { toWorkflowRunState, WorkflowRunStatePublisher } from "./workflow-run-state-publisher.js";
import { makeDecideCompensation } from "./workflow-compensation-control.js";

export type WorkflowRuntimeAcceptanceError = StartWorkflowError;
export type WorkflowRuntimeError = ExecuteWorkflowError;
export type WorkflowRuntimeReadError = WorkflowRunNotFoundError | WorkflowRunAcceptanceError;
export type WorkflowRuntimeSignalError = SignalWorkflowError;
export type WorkflowRuntimeState = PeekResult<Schema.Json, WorkflowRunError>;
export type WorkflowRuntimeInspectError = WorkflowRunNotFoundError | WorkflowRunAcceptanceError;
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
  readonly watch: (
    address: WorkflowRunAddress,
    options?: { readonly interval?: Duration.Input },
  ) => Stream.Stream<WorkflowRuntimeState, WorkflowRuntimeReadError>;
  readonly wait: (
    address: WorkflowRunAddress,
  ) => Effect.Effect<WorkflowRuntimeState, WorkflowRuntimeReadError>;
  readonly inspect: (
    address: WorkflowRunAddress,
  ) => Effect.Effect<WorkflowRunState, WorkflowRuntimeInspectError>;
  readonly interrupt: (
    address: WorkflowRunAddress,
  ) => Effect.Effect<void, WorkflowRuntimeInspectError>;
  readonly decideCompensation: (
    request: DecideWorkflowCompensationRequest,
  ) => Effect.Effect<void, WorkflowRuntimeCompensationError>;
}

export class WorkflowRuntime extends Context.Service<WorkflowRuntime, WorkflowRuntimeShape>()(
  "@cvr/loom-runtime/WorkflowRuntime",
) {}

type AcceptWorkflow = (
  request: WorkflowRunRequest,
) => Effect.Effect<AcceptedWorkflowRun, WorkflowRuntimeAcceptanceError>;

const toExecution = (accepted: AcceptedWorkflowRun) =>
  WorkflowRunExecution.make({
    incarnationId: accepted.incarnationId,
    request: accepted.request,
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

const makeControlWorkflow = (
  acceptance: WorkflowRunAcceptance["Service"],
  engine: WorkflowEngine.WorkflowEngine["Service"],
  publisher: WorkflowRunStatePublisher["Service"],
) => {
  const provideEngine = Effect.provideService(WorkflowEngine.WorkflowEngine, engine);
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
    decideCompensation: makeDecideCompensation(acceptance, engine, publisher),
  };
};

const makeDeclareWorkflow =
  (declarations: WorkflowSignalDeclarations["Service"]) => (accepted: AcceptedWorkflowRun) =>
    declarations.declare(accepted.workflowRunId, accepted.request.definition.signals);

const makePrepareWorkflow =
  (
    accept: AcceptWorkflow,
    declare: ReturnType<typeof makeDeclareWorkflow>,
    storageClient: Client["Service"],
    publisher: WorkflowRunStatePublisher["Service"],
  ) =>
  (request: WorkflowRunRequest) =>
    accept(request).pipe(
      Effect.tap(declare),
      Effect.tap((accepted) => LoomDynamicWorkflow.send(toExecution(accepted))),
      storageClient.withTransaction,
      Effect.tap((accepted) =>
        publisher.watch({
          sessionId: accepted.request.sessionId,
          workflowRunId: accepted.workflowRunId,
        }),
      ),
    );

const makeExecuteWorkflow =
  (
    prepare: ReturnType<typeof makePrepareWorkflow>,
    workflowClient: Effect.Success<typeof LoomDynamicWorkflow.Context>,
  ): WorkflowRuntimeShape["execute"] =>
  (request) =>
    prepare(request).pipe(
      Effect.flatMap((accepted) => LoomDynamicWorkflow.execute(toExecution(accepted))),
      Effect.provideService(LoomDynamicWorkflow.Context, workflowClient),
    );

const makeSendWorkflow =
  (
    prepare: ReturnType<typeof makePrepareWorkflow>,
    workflowClient: Effect.Success<typeof LoomDynamicWorkflow.Context>,
  ): WorkflowRuntimeShape["send"] =>
  (request) =>
    prepare(request).pipe(
      Effect.map(({ workflowRunId }) => workflowRunId),
      Effect.provideService(LoomDynamicWorkflow.Context, workflowClient),
    );

const makeRuntime = (
  accept: AcceptWorkflow,
  acceptance: WorkflowRunAcceptance["Service"],
  workflowClient: Effect.Success<typeof LoomDynamicWorkflow.Context>,
  storageClient: Client["Service"],
  engine: WorkflowEngine.WorkflowEngine["Service"],
  declarations: WorkflowSignalDeclarations["Service"],
  publisher: WorkflowRunStatePublisher["Service"],
): WorkflowRuntimeShape => {
  const provideEngine = Effect.provideService(WorkflowEngine.WorkflowEngine, engine);
  const declare = makeDeclareWorkflow(declarations);
  const prepare = makePrepareWorkflow(accept, declare, storageClient, publisher);
  return {
    execute: makeExecuteWorkflow(prepare, workflowClient),
    send: makeSendWorkflow(prepare, workflowClient),
    signal: makeSignalWorkflow(acceptance, declarations, engine, publisher),
    watch: (address, options) =>
      Stream.unwrap(
        acceptance
          .authorize(address)
          .pipe(
            Effect.as(
              LoomDynamicWorkflow.watchAt(address.workflowRunId, options).pipe(
                Stream.provideService(WorkflowEngine.WorkflowEngine, engine),
              ),
            ),
          ),
      ),
    wait: (address) =>
      acceptance
        .authorize(address)
        .pipe(Effect.andThen(LoomDynamicWorkflow.waitForAt(address.workflowRunId)), provideEngine),
    ...makeControlWorkflow(acceptance, engine, publisher),
  };
};

export const makeWorkflowRuntime = Effect.gen(function* () {
  const acceptance = yield* WorkflowRunAcceptance;
  const workflowClient = yield* LoomDynamicWorkflow.Context;
  const storageClient = yield* Client;
  const engine = yield* WorkflowEngine.WorkflowEngine;
  const declarations = yield* WorkflowSignalDeclarations;
  const publisher = yield* WorkflowRunStatePublisher;
  return WorkflowRuntime.of(
    makeRuntime(
      acceptance.accept,
      acceptance,
      workflowClient,
      storageClient,
      engine,
      declarations,
      publisher,
    ),
  );
});

export const layerWorkflowRuntime = Layer.effect(WorkflowRuntime, makeWorkflowRuntime);
