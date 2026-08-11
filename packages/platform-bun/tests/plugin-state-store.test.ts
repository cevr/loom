import {
  PluginId,
  PluginStateAddress,
  PluginStateKey,
  PluginStateScope,
  SessionId,
} from "@cvr/loom-domain";
import {
  PluginStateReadResult,
  PluginStateRevisionConflictError,
  PluginStateVersion,
} from "@cvr/loom-protocol";
import { PluginStateStore } from "@cvr/loom-runtime";
import { BunServices } from "@effect/platform-bun";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer } from "effect";
import { layerLoomSqlite, layerSqlitePluginStateStore } from "../src/index.js";

const workspace = PluginStateScope.cases.Workspace.make({});
const sessionOne = SessionId.make("session-1");
const sessionTwo = SessionId.make("session-2");
const key = PluginStateKey.make("preferences");

const address = (plugin: string, scope: PluginStateScope) =>
  PluginStateAddress.make({ pluginId: PluginId.make(plugin), scope, key });

const scopedLive = it.scopedLive.layer(BunServices.layer);

const withStore = <A, E, R>(effect: Effect.Effect<A, E, PluginStateStore | R>) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-plugin-state-" });
    const database = layerLoomSqlite({ filename: `${directory}/loom.sqlite` });
    return yield* effect.pipe(
      Effect.provide(layerSqlitePluginStateStore.pipe(Layer.provide(database))),
    );
  });

scopedLive("isolates Plugin State and applies compare-and-set revisions", () =>
  withStore(
    Effect.gen(function* () {
      const store = yield* PluginStateStore;
      const workspaceAddress = address("plugin-a", workspace);
      const otherPluginAddress = address("plugin-b", workspace);

      expect(yield* store.read(workspaceAddress)).toEqual(
        PluginStateReadResult.cases.Missing.make({}),
      );
      expect(
        yield* store.write(workspaceAddress, PluginStateVersion.cases.Missing.make({}), {
          theme: "rose-pine",
        }),
      ).toBe(1);

      const conflict = yield* store
        .write(workspaceAddress, PluginStateVersion.cases.Missing.make({}), { theme: "moon" })
        .pipe(Effect.flip);
      if (!(conflict instanceof PluginStateRevisionConflictError))
        return yield* Effect.die(conflict);
      expect(conflict.actual).toEqual(PluginStateVersion.cases.Present.make({ revision: 1 }));

      expect(
        yield* store.write(
          workspaceAddress,
          PluginStateVersion.cases.Present.make({ revision: 1 }),
          { theme: "moon" },
        ),
      ).toBe(2);
      expect(yield* store.read(workspaceAddress)).toEqual(
        PluginStateReadResult.cases.Present.make({ value: { theme: "moon" }, revision: 2 }),
      );
      expect(yield* store.read(otherPluginAddress)).toEqual(
        PluginStateReadResult.cases.Missing.make({}),
      );

      const missingConflict = yield* store
        .write(otherPluginAddress, PluginStateVersion.cases.Present.make({ revision: 1 }), [
          "compact",
          true,
        ])
        .pipe(Effect.flip);
      if (!(missingConflict instanceof PluginStateRevisionConflictError)) {
        return yield* Effect.die(missingConflict);
      }
      expect(missingConflict.actual).toEqual(PluginStateVersion.cases.Missing.make({}));
    }),
  ),
);

scopedLive("deletes Session Plugin State and preserves other scopes", () =>
  withStore(
    Effect.gen(function* () {
      const store = yield* PluginStateStore;
      const workspaceAddress = address("plugin-a", workspace);
      const sessionOneAddress = address(
        "plugin-a",
        PluginStateScope.cases.Session.make({ sessionId: sessionOne }),
      );
      const sessionTwoAddress = address(
        "plugin-a",
        PluginStateScope.cases.Session.make({ sessionId: sessionTwo }),
      );

      yield* store.write(workspaceAddress, PluginStateVersion.cases.Missing.make({}), {
        theme: "moon",
      });
      yield* store.write(sessionOneAddress, PluginStateVersion.cases.Missing.make({}), {
        draft: 1,
      });
      yield* store.write(sessionTwoAddress, PluginStateVersion.cases.Missing.make({}), {
        draft: 2,
      });
      yield* store.deleteSession(sessionOne);

      expect(yield* store.read(sessionOneAddress)).toEqual(
        PluginStateReadResult.cases.Missing.make({}),
      );
      expect(yield* store.read(sessionTwoAddress)).toHaveProperty("_tag", "Present");
      expect(yield* store.read(workspaceAddress)).toHaveProperty("_tag", "Present");
    }),
  ),
);
