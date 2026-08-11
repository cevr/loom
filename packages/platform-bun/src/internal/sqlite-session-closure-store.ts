import { SessionId } from "@cvr/loom-domain";
import {
  SessionClosureStore,
  SessionClosureStoreError,
  type SessionClosureStoreShape,
} from "@cvr/loom-runtime";
import { Clock, Duration, Effect, Layer, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";

const ClosureLookup = Schema.Struct({ closed: Schema.BooleanFromBit });
const SessionClosure = Schema.Struct({ sessionId: SessionId });

const storeError = (
  operation: SessionClosureStoreError["operation"],
  cause: unknown,
): SessionClosureStoreError => new SessionClosureStoreError({ operation, cause });

const makeLookup = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Schema.Struct({ sessionId: SessionId, now: Schema.Int }),
    Result: ClosureLookup,
    execute: ({ sessionId, now }) => sql`
      SELECT EXISTS(
        SELECT 1 FROM session_closures
        WHERE session_id = ${sessionId} AND retain_until > ${now}
      ) AS closed
    `,
  });

const makeList = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Int,
    Result: SessionClosure,
    execute: (now) => sql`
      SELECT session_id AS sessionId
      FROM session_closures
      WHERE retain_until > ${now}
      ORDER BY session_id
    `,
  });

const makeClose = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteSessionClosureStore.close")(
    function* (sessionId: SessionId, lease: Duration.Input) {
      const now = yield* Clock.currentTimeMillis;
      const retainUntil = now + Duration.toMillis(lease);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`DELETE FROM session_closures WHERE retain_until <= ${now}`;
          yield* sql`
            INSERT INTO session_closures (session_id, retain_until)
            VALUES (${sessionId}, ${retainUntil})
            ON CONFLICT(session_id) DO NOTHING
          `;
        }),
      );
    },
    Effect.mapError((cause) => storeError("close", cause)),
  );

const makePrune = (sql: SqlClient.SqlClient) => {
  const prune = SqlSchema.findAll({
    Request: Schema.Int,
    Result: SessionClosure,
    execute: (now) => sql`
      DELETE FROM session_closures
      WHERE retain_until <= ${now}
      RETURNING session_id AS sessionId
    `,
  });
  return Clock.currentTimeMillis.pipe(
    Effect.flatMap(prune),
    Effect.map((rows) => rows.length),
    Effect.mapError((cause) => storeError("prune", cause)),
  );
};

export const makeSqliteSessionClosureStore: Effect.Effect<
  SessionClosureStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const lookup = makeLookup(sql);
  const list = makeList(sql);

  return SessionClosureStore.of({
    close: makeClose(sql),
    contains: Effect.fn("SqliteSessionClosureStore.contains")(
      function* (sessionId: SessionId) {
        const now = yield* Clock.currentTimeMillis;
        return (yield* lookup({ sessionId, now })).closed;
      },
      Effect.mapError((cause) => storeError("contains", cause)),
    ),
    list: Clock.currentTimeMillis.pipe(
      Effect.flatMap(list),
      Effect.map((rows) => rows.map(({ sessionId }) => sessionId)),
      Effect.mapError((cause) => storeError("list", cause)),
    ),
    prune: makePrune(sql),
  });
});

export const layerSqliteSessionClosureStore = Layer.effect(
  SessionClosureStore,
  makeSqliteSessionClosureStore,
);
