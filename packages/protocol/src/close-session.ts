import { SessionId } from "@cvr/loom-domain";
import { Rpc } from "effect/unstable/rpc";
import { CloseSessionError } from "./close-session-error.js";

export class CloseSession extends Rpc.make("Session.Close", {
  payload: { sessionId: SessionId },
  error: CloseSessionError,
}) {}
