import { WorkspaceRoot } from "@cvr/loom-domain";
import { Schema } from "effect";
import { ProtocolVersion } from "./protocol-version.js";

export const HandshakeSuccess = Schema.Struct({
  workspaceRoot: WorkspaceRoot,
  protocolVersion: ProtocolVersion,
  maximumFrameSize: Schema.Int.check(Schema.isGreaterThan(0)),
  daemonStartedAtMillis: Schema.Natural,
});
export type HandshakeSuccess = typeof HandshakeSuccess.Type;
