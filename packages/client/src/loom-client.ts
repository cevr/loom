import type { AgentOwner, WorkflowRunRequest } from "@cvr/loom-domain";
import type {
  CellEvaluation,
  CellEvaluationError,
  EvaluateCellRequest,
  ExecuteWorkflowError,
  HandshakeError,
  HandshakeSuccess,
  SignalWorkflowError,
  SignalWorkflowRequest,
  StartWorkflowError,
  WorkflowRunHandle,
} from "@cvr/loom-protocol";
import { Context, type Effect, type Schema } from "effect";
import type { DaemonUnavailableError } from "./daemon-unavailable-error.js";
import type { MessageTooLargeError } from "./message-too-large-error.js";

export interface LoomClientShape {
  readonly handshake: Effect.Effect<HandshakeSuccess, HandshakeError | DaemonUnavailableError>;
  readonly evaluateCell: (
    request: EvaluateCellRequest,
  ) => Effect.Effect<
    CellEvaluation,
    CellEvaluationError | HandshakeError | DaemonUnavailableError | MessageTooLargeError
  >;
  readonly resetCodeKernel: (
    owner: AgentOwner,
  ) => Effect.Effect<void, HandshakeError | DaemonUnavailableError>;
  readonly executeWorkflow: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<Schema.Json, ExecuteWorkflowError | HandshakeError | DaemonUnavailableError>;
  readonly startWorkflow: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<
    WorkflowRunHandle,
    StartWorkflowError | HandshakeError | DaemonUnavailableError
  >;
  readonly signalWorkflow: (
    request: SignalWorkflowRequest,
  ) => Effect.Effect<void, SignalWorkflowError | HandshakeError | DaemonUnavailableError>;
}

export class LoomClient extends Context.Service<LoomClient, LoomClientShape>()(
  "@cvr/loom-client/LoomClient",
) {}
