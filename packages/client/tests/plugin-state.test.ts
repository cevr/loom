import { PluginId, PluginStateKey, PluginStateScope } from "@cvr/loom-domain";
import { PluginStateReadResult, PluginStateVersion } from "@cvr/loom-protocol";
import { expect, it } from "effect-bun-test";
import { Effect, Exit, Option, Schema } from "effect";
import type { LoomClientShape } from "../src/loom-client.js";
import { makePluginState } from "../src/plugin-state.js";

const Preferences = Schema.Struct({ theme: Schema.Literals(["rose-pine", "rose-pine-moon"]) });
const pluginId = PluginId.make("appearance");
const key = PluginStateKey.make("preferences");
const scope = PluginStateScope.cases.Workspace.make({});

it.effect("validates Plugin-owned schemas on both sides of RPC", () =>
  Effect.gen(function* () {
    let written: Schema.Json = {};
    const client: Pick<LoomClientShape, "readPluginState" | "writePluginState"> = {
      readPluginState: () =>
        Effect.succeed(PluginStateReadResult.cases.Present.make({ value: written, revision: 1 })),
      writePluginState: ({ expected, value }) =>
        Effect.sync(() => {
          expect(expected).toEqual(PluginStateVersion.cases.Missing.make({}));
          written = value;
          return 1;
        }),
    };
    const state = makePluginState(client, pluginId, scope, Preferences);

    expect(yield* state.write(key, { theme: "rose-pine" }, Option.none())).toBe(1);
    expect(yield* state.read(key)).toEqual(
      Option.some({ value: { theme: "rose-pine" }, revision: 1 }),
    );

    written = { theme: "invalid" };
    expect(Exit.isFailure(yield* state.read(key).pipe(Effect.exit))).toBe(true);
  }),
);
