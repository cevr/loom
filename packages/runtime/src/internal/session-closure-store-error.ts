import { Schema } from "effect";

export class SessionClosureStoreError extends Schema.TaggedError<SessionClosureStoreError>()(
  "SessionClosureStoreError",
  {
    operation: Schema.Literals(["close", "contains", "list", "prune"]),
    cause: Schema.Defect(),
  },
) {}
