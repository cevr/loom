import { Schema } from "effect";

export class DaemonUnavailableError extends Schema.TaggedError<DaemonUnavailableError>()(
  "DaemonUnavailableError",
  {
    operation: Schema.String,
    socketPath: Schema.NonEmptyString,
    cause: Schema.Defect(),
  },
) {}
