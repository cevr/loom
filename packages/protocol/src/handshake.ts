import { Schema } from "effect";
import { Rpc } from "effect/unstable/rpc";
import { HandshakeRequest } from "./handshake-request.js";
import { HandshakeSuccess } from "./handshake-success.js";
import { IncompatibleProtocolError } from "./incompatible-protocol-error.js";
import { WorkspaceMismatchError } from "./workspace-mismatch-error.js";

export const HandshakeError = Schema.Union([IncompatibleProtocolError, WorkspaceMismatchError]);
export type HandshakeError = typeof HandshakeError.Type;

export class Handshake extends Rpc.make("Connection.Handshake", {
  payload: HandshakeRequest,
  success: HandshakeSuccess,
  error: HandshakeError,
}) {}
