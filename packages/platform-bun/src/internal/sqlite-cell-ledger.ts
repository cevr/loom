import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import {
  CellEvaluation,
  CellInterruptedError,
  CellLedgerEntry,
  CellLedgerState,
} from "@cvr/loom-protocol";
import {
  CellLedger,
  CellLedgerClaim,
  CellLedgerStoreError,
  type CellLedgerShape,
  type CellTerminalOutcome,
} from "@cvr/loom-runtime";
import { Effect, Layer, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";

const CellAddress = Schema.Struct({ sessionId: SessionId, agentId: AgentId, cellId: CellId });
const CellLedgerRow = Schema.Struct({
  ...CellAddress.fields,
  source: Schema.String,
  state: Schema.String,
});
const ChangeCount = Schema.Struct({ count: Schema.Int });
const encodeState = Schema.encodeEffect(Schema.fromJsonString(CellLedgerState));
const decodeState = Schema.decodeUnknownEffect(Schema.fromJsonString(CellLedgerState));
const isEvaluation = Schema.is(CellEvaluation);
const isInterrupted = Schema.is(CellInterruptedError);

const storeError = (
  operation: CellLedgerStoreError["operation"],
  cause: unknown,
): CellLedgerStoreError => new CellLedgerStoreError({ operation, cause });

const makeSelect = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: CellAddress,
    Result: CellLedgerRow,
    execute: (address) => sql`
      SELECT session_id AS sessionId, agent_id AS agentId, cell_id AS cellId, source, state_json AS state
      FROM cell_ledger
      WHERE session_id = ${address.sessionId}
        AND agent_id = ${address.agentId}
        AND cell_id = ${address.cellId}
    `,
  });

const makeListActive = (sql: SqlClient.SqlClient) =>
  SqlSchema.findAll({
    Request: Schema.Void,
    Result: CellLedgerRow,
    execute: () => sql`
      SELECT session_id AS sessionId, agent_id AS agentId, cell_id AS cellId, source, state_json AS state
      FROM cell_ledger
      WHERE json_extract(state_json, '$._tag') IN ('Accepted', 'Evaluating')
      ORDER BY rowid
    `,
  });

const makeChangeCount = (sql: SqlClient.SqlClient) =>
  SqlSchema.findOne({
    Request: Schema.Void,
    Result: ChangeCount,
    execute: () => sql`SELECT changes() AS count`,
  });

const decodeRow = Effect.fn("SqliteCellLedger.decodeRow")(function* (
  row: typeof CellLedgerRow.Type,
) {
  const state = yield* decodeState(row.state);
  return CellLedgerEntry.make({ ...row, state });
});

const makeUpdateState = (sql: SqlClient.SqlClient) =>
  Effect.fn("SqliteCellLedger.updateState")(function* (
    entry: CellLedgerEntry,
    expected: "Accepted" | "Evaluating",
    state: CellLedgerState,
  ) {
    const encoded = yield* encodeState(state);
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`
          UPDATE cell_ledger SET state_json = ${encoded}
          WHERE session_id = ${entry.sessionId}
            AND agent_id = ${entry.agentId}
            AND cell_id = ${entry.cellId}
            AND source = ${entry.source}
            AND json_extract(state_json, '$._tag') = ${expected}
        `;
        return (yield* makeChangeCount(sql)()).count === 1;
      }),
    );
  });

const makeClaim = (sql: SqlClient.SqlClient) => {
  const select = makeSelect(sql);
  const changeCount = makeChangeCount(sql);
  return Effect.fn("SqliteCellLedger.claim")(function* (entry: CellLedgerEntry) {
    const encoded = yield* encodeState(entry.state);
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`
          INSERT INTO cell_ledger (session_id, agent_id, cell_id, source, state_json)
          VALUES (${entry.sessionId}, ${entry.agentId}, ${entry.cellId}, ${entry.source}, ${encoded})
          ON CONFLICT(session_id, agent_id, cell_id) DO NOTHING
        `;
        const { count } = yield* changeCount();
        const stored = yield* select(entry).pipe(Effect.flatMap(decodeRow));
        if (count === 1) return CellLedgerClaim.Accepted({ entry: stored });
        return CellLedgerClaim.Existing({ entry: stored });
      }),
    );
  });
};

const makeComplete = (updateState: ReturnType<typeof makeUpdateState>) =>
  Effect.fn("SqliteCellLedger.complete")(function* (
    entry: CellLedgerEntry,
    outcome: CellTerminalOutcome,
  ) {
    if (isEvaluation(outcome)) {
      return yield* updateState(
        entry,
        "Evaluating",
        CellLedgerState.cases.Succeeded.make({ evaluation: outcome }),
      );
    }
    if (isInterrupted(outcome)) {
      return yield* updateState(
        entry,
        "Evaluating",
        CellLedgerState.cases.Interrupted.make({ error: outcome }),
      );
    }
    return yield* updateState(
      entry,
      "Evaluating",
      CellLedgerState.cases.Failed.make({ error: outcome }),
    );
  });

const makeReconcile = (sql: SqlClient.SqlClient, updateState: ReturnType<typeof makeUpdateState>) =>
  makeListActive(sql)().pipe(
    Effect.flatMap(
      Effect.forEach(
        (row) =>
          decodeRow(row).pipe(
            Effect.flatMap((entry) => {
              const interrupted = CellLedgerState.cases.Interrupted.make({
                error: new CellInterruptedError({
                  cellId: entry.cellId,
                  reason: "DaemonRestart",
                  message: "The daemon restarted before the Cell reached a terminal outcome.",
                }),
              });
              if (CellLedgerState.guards.Accepted(entry.state)) {
                return updateState(entry, "Accepted", interrupted);
              }
              return updateState(entry, "Evaluating", interrupted);
            }),
          ),
        { discard: true },
      ),
    ),
  );

export const makeSqliteCellLedger: Effect.Effect<CellLedgerShape, never, SqlClient.SqlClient> =
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const claim = makeClaim(sql);
    const updateState = makeUpdateState(sql);
    const complete = makeComplete(updateState);
    return CellLedger.of({
      claim: (entry) =>
        claim(entry).pipe(
          Effect.catchTags({ NoSuchElementError: Effect.die, SchemaError: Effect.die }),
          Effect.mapError((cause) => storeError("claim", cause)),
        ),
      evaluating: (entry) =>
        updateState(entry, "Accepted", CellLedgerState.cases.Evaluating.make({})).pipe(
          Effect.mapError((cause) => storeError("transition", cause)),
          Effect.filterOrFail(
            (changed) => changed,
            () => storeError("transition", "The Cell is not Accepted."),
          ),
          Effect.asVoid,
        ),
      complete: (entry, outcome) =>
        complete(entry, outcome).pipe(
          Effect.mapError((cause) => storeError("transition", cause)),
          Effect.filterOrFail(
            (changed) => changed,
            () => storeError("transition", "The Cell is not Evaluating."),
          ),
          Effect.asVoid,
        ),
      reconcile: makeReconcile(sql, updateState).pipe(
        Effect.catchTag("SchemaError", Effect.die),
        Effect.mapError((cause) => storeError("reconcile", cause)),
      ),
    });
  });

export const layerSqliteCellLedger: Layer.Layer<CellLedger, never, SqlClient.SqlClient> =
  Layer.effect(CellLedger, makeSqliteCellLedger);
