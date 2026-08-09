import { WorkspaceRoot } from "@cvr/loom-domain";
import { Schema } from "effect";
import { ProtocolVersion } from "./protocol-version.js";

export const HandshakeRequest = Schema.Struct({
  workspaceRoot: WorkspaceRoot,
  minimumProtocolVersion: ProtocolVersion,
  maximumProtocolVersion: ProtocolVersion,
});
export type HandshakeRequest = typeof HandshakeRequest.Type;
