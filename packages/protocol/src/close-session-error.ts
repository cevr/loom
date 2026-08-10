import { SessionId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class CloseSessionError extends Schema.TaggedError<CloseSessionError>()(
  "CloseSessionError",
  {
    sessionId: SessionId,
    message: Schema.String,
  },
) {}
