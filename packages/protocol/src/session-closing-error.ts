import { SessionId } from "@cvr/loom-domain";
import { Schema } from "effect";

export class SessionClosingError extends Schema.TaggedError<SessionClosingError>()(
  "SessionClosingError",
  { sessionId: SessionId },
) {
  override get message(): string {
    return "The Session is closing.";
  }
}
