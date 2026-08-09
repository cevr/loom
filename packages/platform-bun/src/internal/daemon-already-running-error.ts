import { Schema } from "effect";

export class DaemonAlreadyRunningError extends Schema.TaggedError<DaemonAlreadyRunningError>()(
  "DaemonAlreadyRunningError",
  {
    socketPath: Schema.NonEmptyString,
  },
) {}
