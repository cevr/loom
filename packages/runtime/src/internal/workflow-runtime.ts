import type { WorkflowRunRequest } from "@cvr/loom-domain";
import type {
  WorkflowIdentityConflictError,
  WorkflowRunAcceptanceError,
  WorkflowRunError,
} from "@cvr/loom-protocol";
import { Context, Effect, Layer, Schema, Stream, type Duration } from "effect";
import { WorkflowEngine } from "effect/unstable/workflow";
import type { ExecId, PeekResult } from "effect-encore";
import { LoomDynamicWorkflow } from "./loom-dynamic-workflow.js";
import { WorkflowRunAcceptance } from "./workflow-run-acceptance.js";

export type WorkflowRuntimeAcceptanceError =
  | WorkflowIdentityConflictError
  | WorkflowRunAcceptanceError;
export type WorkflowRuntimeError = WorkflowRuntimeAcceptanceError | WorkflowRunError;
export type WorkflowRuntimeExecutionId = ExecId<Schema.Json, WorkflowRunError>;
export type WorkflowRuntimeState = PeekResult<Schema.Json, WorkflowRunError>;

export interface WorkflowRuntimeShape {
  readonly execute: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<Schema.Json, WorkflowRuntimeError>;
  readonly send: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<WorkflowRuntimeExecutionId, WorkflowRuntimeAcceptanceError>;
  readonly peek: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<WorkflowRuntimeState, WorkflowRuntimeAcceptanceError>;
  readonly watch: (
    request: WorkflowRunRequest,
    options?: { readonly interval?: Duration.Input },
  ) => Stream.Stream<WorkflowRuntimeState, WorkflowRuntimeAcceptanceError>;
  readonly wait: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<WorkflowRuntimeState, WorkflowRuntimeAcceptanceError>;
  readonly interrupt: (executionId: string) => Effect.Effect<void>;
  readonly resume: (executionId: string) => Effect.Effect<void>;
}

export class WorkflowRuntime extends Context.Service<WorkflowRuntime, WorkflowRuntimeShape>()(
  "@cvr/loom-runtime/WorkflowRuntime",
) {}

export const makeWorkflowRuntime = Effect.gen(function* () {
  const acceptance = yield* WorkflowRunAcceptance;
  const client = yield* LoomDynamicWorkflow.Context;
  const engine = yield* WorkflowEngine.WorkflowEngine;

  const accept = (received: WorkflowRunRequest) =>
    Effect.map(acceptance.accept(received), ({ request }) => request);
  const provideClient = Effect.provideService(LoomDynamicWorkflow.Context, client);
  const provideEngine = Effect.provideService(WorkflowEngine.WorkflowEngine, engine);

  return WorkflowRuntime.of({
    execute: (request) =>
      accept(request).pipe(Effect.flatMap(LoomDynamicWorkflow.execute), provideClient),
    send: (request) =>
      accept(request).pipe(Effect.flatMap(LoomDynamicWorkflow.send), provideClient),
    peek: (request) =>
      accept(request).pipe(Effect.flatMap(LoomDynamicWorkflow.peek), provideEngine),
    watch: (request, options) =>
      Stream.unwrap(
        accept(request).pipe(
          Effect.map((accepted) =>
            LoomDynamicWorkflow.watch(accepted, options).pipe(
              Stream.provideService(WorkflowEngine.WorkflowEngine, engine),
            ),
          ),
        ),
      ),
    wait: (request) =>
      accept(request).pipe(Effect.flatMap(LoomDynamicWorkflow.waitFor), provideEngine),
    interrupt: (executionId) => LoomDynamicWorkflow.interrupt(executionId).pipe(provideEngine),
    resume: (executionId) => LoomDynamicWorkflow.resume(executionId).pipe(provideEngine),
  });
});

export const layerWorkflowRuntime = Layer.effect(WorkflowRuntime, makeWorkflowRuntime);
