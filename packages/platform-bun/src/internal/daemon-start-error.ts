import { Schema } from "effect";

export class DaemonStartError extends Schema.TaggedError<DaemonStartError>()("DaemonStartError", {
  entryPath: Schema.NonEmptyString,
  workspaceRoot: Schema.NonEmptyString,
  cause: Schema.Defect(),
}) {}
