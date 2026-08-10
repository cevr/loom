import { Schema } from "effect";

export const DaemonUnavailableReason = Schema.Literals([
  "ConnectionTimeout",
  "RequestTimeout",
  "TransportFailure",
]);
export type DaemonUnavailableReason = typeof DaemonUnavailableReason.Type;

export class DaemonUnavailableError extends Schema.TaggedError<DaemonUnavailableError>()(
  "DaemonUnavailableError",
  {
    operation: Schema.String,
    socketPath: Schema.NonEmptyString,
    reason: DaemonUnavailableReason,
    cause: Schema.Option(Schema.Defect()),
  },
) {}
