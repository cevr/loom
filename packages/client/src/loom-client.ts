import type { AgentOwner, WorkflowRunAddress, WorkflowRunRequest } from "@cvr/loom-domain";
import type {
  CellEvaluation,
  CellEvaluationError,
  CloseSessionError,
  DecideWorkflowCompensationRequest,
  DecideWorkflowCompensationError,
  EvaluateCellRequest,
  ExecuteWorkflowError,
  HandshakeError,
  HandshakeSuccess,
  SignalWorkflowError,
  SignalWorkflowRequest,
  StartWorkflowError,
  InspectWorkflowError,
  WorkflowRunHandle,
  WorkflowRunState,
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
  readonly closeSession: (
    sessionId: AgentOwner["sessionId"],
  ) => Effect.Effect<void, CloseSessionError | HandshakeError | DaemonUnavailableError>;
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
  readonly inspectWorkflow: (
    request: WorkflowRunAddress,
  ) => Effect.Effect<
    WorkflowRunState,
    InspectWorkflowError | HandshakeError | DaemonUnavailableError
  >;
  readonly interruptWorkflow: (
    request: WorkflowRunAddress,
  ) => Effect.Effect<void, InspectWorkflowError | HandshakeError | DaemonUnavailableError>;
  readonly decideWorkflowCompensation: (
    request: DecideWorkflowCompensationRequest,
  ) => Effect.Effect<
    void,
    DecideWorkflowCompensationError | HandshakeError | DaemonUnavailableError
  >;
}

export class LoomClient extends Context.Service<LoomClient, LoomClientShape>()(
  "@cvr/loom-client/LoomClient",
) {}
