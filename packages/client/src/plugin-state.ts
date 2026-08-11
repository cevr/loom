import type { PluginId, PluginStateKey, PluginStateScope } from "@cvr/loom-domain";
import {
  PluginStateReadResult,
  type PluginStateRevision,
  PluginStateVersion,
} from "@cvr/loom-protocol";
import { Effect, Option, Schema } from "effect";
import type { LoomClientShape } from "./loom-client.js";

export interface PluginStateValue<A> {
  readonly value: A;
  readonly revision: PluginStateRevision;
}

export const makePluginState = <A>(
  client: Pick<LoomClientShape, "readPluginState" | "writePluginState">,
  pluginId: PluginId,
  scope: PluginStateScope,
  schema: Schema.Codec<A, Schema.Json>,
) => ({
  read: Effect.fn("PluginState.read")(function* (key: PluginStateKey) {
    const result = yield* client.readPluginState({ address: { pluginId, scope, key } });
    if (PluginStateReadResult.guards.Missing(result)) return Option.none<PluginStateValue<A>>();
    return Option.some({
      value: yield* Schema.decodeEffect(schema)(result.value),
      revision: result.revision,
    });
  }),
  write: Effect.fn("PluginState.write")(function* (
    key: PluginStateKey,
    value: A,
    expectedRevision: Option.Option<PluginStateRevision>,
  ) {
    const expected = Option.match(expectedRevision, {
      onNone: () => PluginStateVersion.cases.Missing.make({}),
      onSome: (revision) => PluginStateVersion.cases.Present.make({ revision }),
    });
    return yield* client.writePluginState({
      address: { pluginId, scope, key },
      expected,
      value: yield* Schema.encodeEffect(schema)(value),
    });
  }),
});
