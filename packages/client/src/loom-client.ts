import type {
  CellEvaluation,
  CellEvaluationError,
  EvaluateCellRequest,
  HandshakeError,
  HandshakeSuccess,
} from "@cvr/loom-protocol";
import { Context, type Effect } from "effect";
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
}

export class LoomClient extends Context.Service<LoomClient, LoomClientShape>()(
  "@cvr/loom-client/LoomClient",
) {}
import type { AgentOwner } from "@cvr/loom-domain";
