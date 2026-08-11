import {
  type SessionId,
  type WorkflowRunAddress,
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
  WorkflowRunState,
  type WorkflowRunNotFoundError,
} from "@cvr/loom-protocol";
import { Context, Effect, Layer, Schema, Stream, type Duration } from "effect";
import { WorkflowEngine } from "effect/unstable/workflow";
import { LoomDynamicWorkflow, loomWorkflowSignal } from "./loom-dynamic-workflow.js";
import { WorkflowRunAcceptance } from "./workflow-run-acceptance.js";
import { WorkflowSignalDeclarations } from "./workflow-signal-declarations.js";
import { toWorkflowRunState, WorkflowRunStatePublisher } from "./workflow-run-state-publisher.js";
import { makeDecideCompensation } from "./workflow-compensation-control.js";
import {
  makePrepareWorkflow,
  type PrepareWorkflow,
  toExecution,
} from "./workflow-run-preparation.js";

export type WorkflowRuntimeAcceptanceError = StartWorkflowError;
export type WorkflowRuntimeError = ExecuteWorkflowError;
export type WorkflowRuntimeReadError = WorkflowRunNotFoundError | WorkflowRunAcceptanceError;
export type WorkflowRuntimeSignalError = SignalWorkflowError;
export type WorkflowRuntimeState = Effect.Success<ReturnType<typeof LoomDynamicWorkflow.peekAt>>;
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
  readonly closeSession: (sessionId: SessionId) => Effect.Effect<void, WorkflowRunAcceptanceError>;
}

export class WorkflowRuntime extends Context.Service<WorkflowRuntime, WorkflowRuntimeShape>()(
  "@cvr/loom-runtime/WorkflowRuntime",
) {}

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

const makeExecuteWorkflow =
  (
    prepare: PrepareWorkflow,
    workflowClient: Effect.Success<typeof LoomDynamicWorkflow.Context>,
  ): WorkflowRuntimeShape["execute"] =>
  (request) =>
    prepare(request).pipe(
      Effect.flatMap((accepted) => LoomDynamicWorkflow.execute(toExecution(accepted))),
      Effect.provideService(LoomDynamicWorkflow.Context, workflowClient),
    );

const makeSendWorkflow =
  (
    prepare: PrepareWorkflow,
    workflowClient: Effect.Success<typeof LoomDynamicWorkflow.Context>,
  ): WorkflowRuntimeShape["send"] =>
  (request) =>
    prepare(request).pipe(
      Effect.map(({ workflowRunId }) => workflowRunId),
      Effect.provideService(LoomDynamicWorkflow.Context, workflowClient),
    );

const makeRuntime = (
  prepare: PrepareWorkflow,
  acceptance: WorkflowRunAcceptance["Service"],
  workflowClient: Effect.Success<typeof LoomDynamicWorkflow.Context>,
  engine: WorkflowEngine.WorkflowEngine["Service"],
  declarations: WorkflowSignalDeclarations["Service"],
  publisher: WorkflowRunStatePublisher["Service"],
): WorkflowRuntimeShape => {
  const provideEngine = Effect.provideService(WorkflowEngine.WorkflowEngine, engine);
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
    closeSession: (sessionId) =>
      acceptance.listActive.pipe(
        Effect.flatMap((addresses) =>
          Effect.forEach(
            addresses.filter((address) => address.sessionId === sessionId),
            (address) => LoomDynamicWorkflow.interrupt(address.workflowRunId),
            { concurrency: "unbounded", discard: true },
          ),
        ),
        provideEngine,
      ),
    ...makeControlWorkflow(acceptance, engine, publisher),
  };
};

export const makeWorkflowRuntime = Effect.gen(function* () {
  const acceptance = yield* WorkflowRunAcceptance;
  const workflowClient = yield* LoomDynamicWorkflow.Context;
  const engine = yield* WorkflowEngine.WorkflowEngine;
  const declarations = yield* WorkflowSignalDeclarations;
  const publisher = yield* WorkflowRunStatePublisher;
  const prepare = yield* makePrepareWorkflow(acceptance.accept, publisher, declarations);
  return WorkflowRuntime.of(
    makeRuntime(prepare, acceptance, workflowClient, engine, declarations, publisher),
  );
});

export const layerWorkflowRuntime = Layer.effect(WorkflowRuntime, makeWorkflowRuntime);
