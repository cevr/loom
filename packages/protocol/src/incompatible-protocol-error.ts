import { Schema } from "effect";
import { ProtocolVersion } from "./protocol-version.js";

export class IncompatibleProtocolError extends Schema.TaggedError<IncompatibleProtocolError>()(
  "IncompatibleProtocolError",
  {
    clientMinimum: ProtocolVersion,
    clientMaximum: ProtocolVersion,
    daemonMinimum: ProtocolVersion,
    daemonMaximum: ProtocolVersion,
  },
) {}
