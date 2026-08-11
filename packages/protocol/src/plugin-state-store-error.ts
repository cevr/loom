import { Schema } from "effect";

export class PluginStateStoreError extends Schema.TaggedError<PluginStateStoreError>()(
  "PluginStateStoreError",
  {
    operation: Schema.Literals(["read", "write", "deleteSession"]),
    message: Schema.String,
  },
) {}
