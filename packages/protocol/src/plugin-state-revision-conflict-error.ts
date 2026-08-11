import { PluginStateAddress } from "@cvr/loom-domain";
import { Schema } from "effect";
import { PluginStateVersion } from "./plugin-state.js";

export class PluginStateRevisionConflictError extends Schema.TaggedError<PluginStateRevisionConflictError>()(
  "PluginStateRevisionConflictError",
  {
    address: PluginStateAddress,
    expected: PluginStateVersion,
    actual: PluginStateVersion,
  },
) {}
