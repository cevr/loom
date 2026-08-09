import { Schema } from "effect";

export class MessageTooLargeError extends Schema.TaggedError<MessageTooLargeError>()(
  "MessageTooLargeError",
  {
    operation: Schema.String,
    size: Schema.Natural,
    maximum: Schema.Natural,
  },
) {}
