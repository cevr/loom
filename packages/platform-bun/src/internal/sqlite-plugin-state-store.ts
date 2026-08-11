import { PluginStateAddress, type SessionId } from "@cvr/loom-domain";
import {
  PluginStateReadResult,
  PluginStateRevision,
  PluginStateRevisionConflictError,
  PluginStateStoreError,
  PluginStateVersion,
} from "@cvr/loom-protocol";
import { PluginStateStore, type PluginStateStoreShape } from "@cvr/loom-runtime";
import { Effect, Inspectable, Layer, Option, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";

const PluginStateRowAddress = Schema.Struct({
  pluginId: PluginStateAddress.fields.pluginId,
  scope: Schema.Literals(["Workspace", "Session"]),
  ownerId: Schema.String,
  key: PluginStateAddress.fields.key,
});

const PluginStateRow = Schema.Struct({
  value: Schema.String,
  revision: Schema.Int.check(Schema.isGreaterThan(0)),
});

const ChangeCount = Schema.Struct({ count: Schema.Int });
const encodeValue = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));
const decodeValue = Schema.decodeEffect(Schema.fromJsonString(Schema.Json));

const rowAddress = (address: PluginStateAddress): typeof PluginStateRowAddress.Type => {
  if (PluginStateAddress.fields.scope.guards.Workspace(address.scope)) {
    return { pluginId: address.pluginId, scope: "Workspace", ownerId: "", key: address.key };
  }
  return {
    pluginId: address.pluginId,
    scope: "Session",
    ownerId: address.scope.sessionId,
    key: address.key,
  };
};

const nextRevision = (expected: PluginStateVersion) => {
  if (PluginStateVersion.guards.Missing(expected)) return 1;
  return expected.revision + 1;
};

const version = (row: Option.Option<typeof PluginStateRow.Type>): PluginStateVersion =>
  Option.match(row, {
    onNone: () => PluginStateVersion.cases.Missing.make({}),
    onSome: ({ revision }) => PluginStateVersion.cases.Present.make({ revision }),
  });

const storeError = (
  operation: PluginStateStoreError["operation"],
  cause: unknown,
): PluginStateStoreError =>
  new PluginStateStoreError({ operation, message: Inspectable.toStringUnknown(cause) });

const makeSelect = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOneOption({
    Request: PluginStateRowAddress,
    Result: PluginStateRow,
    execute: ({ pluginId, scope, ownerId, key }) => sql`
      SELECT value_json AS value, revision
      FROM plugin_state
      WHERE plugin_id = ${pluginId}
        AND scope = ${scope}
        AND owner_id = ${ownerId}
        AND state_key = ${key}
    `,
  });

const makeChangeCount = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Schema.Void,
    Result: ChangeCount,
    execute: () => sql`SELECT changes() AS count`,
  });

const makeRead = (sql: SqlClient.SqlClient) => {
  const select = makeSelect(sql);
  return Effect.fn("SqlitePluginStateStore.read")(
    function* (address: PluginStateAddress) {
      const row = yield* select(rowAddress(address));
      if (Option.isNone(row)) return PluginStateReadResult.cases.Missing.make({});
      return PluginStateReadResult.cases.Present.make({
        value: yield* decodeValue(row.value.value),
        revision: row.value.revision,
      });
    },
    Effect.mapError((cause) => storeError("read", cause)),
  );
};

const makeWrite = (sql: SqlClient.SqlClient) => {
  const select = makeSelect(sql);
  const changes = makeChangeCount(sql);
  return Effect.fn("SqlitePluginStateStore.write")(
    function* (address: PluginStateAddress, expected: PluginStateVersion, value: Schema.Json) {
      const row = rowAddress(address);
      const encoded = yield* encodeValue(value);
      const revision = nextRevision(expected);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          if (PluginStateVersion.guards.Missing(expected)) {
            yield* sql`
              INSERT INTO plugin_state (
                plugin_id, scope, owner_id, state_key, value_json, revision
              ) VALUES (
                ${row.pluginId}, ${row.scope}, ${row.ownerId}, ${row.key}, ${encoded}, ${revision}
              ) ON CONFLICT(plugin_id, scope, owner_id, state_key) DO NOTHING
            `;
          } else {
            yield* sql`
              UPDATE plugin_state
              SET value_json = ${encoded}, revision = ${revision}
              WHERE plugin_id = ${row.pluginId}
                AND scope = ${row.scope}
                AND owner_id = ${row.ownerId}
                AND state_key = ${row.key}
                AND revision = ${expected.revision}
            `;
          }
          if ((yield* changes()).count === 1) return;
          return yield* new PluginStateRevisionConflictError({
            address,
            expected,
            actual: version(yield* select(row)),
          });
        }),
      );
      return PluginStateRevision.make(revision);
    },
    Effect.mapError((cause) => {
      if (cause instanceof PluginStateRevisionConflictError) return cause;
      return storeError("write", cause);
    }),
  );
};

const makeDeleteSession = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqlitePluginStateStore.deleteSession")(
    function* (sessionId: SessionId) {
      yield* sql`
        DELETE FROM plugin_state WHERE scope = 'Session' AND owner_id = ${sessionId}
      `;
    },
    Effect.mapError((cause) => storeError("deleteSession", cause)),
  );

export const makeSqlitePluginStateStore: Effect.Effect<
  PluginStateStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return PluginStateStore.of({
    read: makeRead(sql),
    write: makeWrite(sql),
    deleteSession: makeDeleteSession(sql),
  });
});

export const layerSqlitePluginStateStore = Layer.effect(
  PluginStateStore,
  makeSqlitePluginStateStore,
);
