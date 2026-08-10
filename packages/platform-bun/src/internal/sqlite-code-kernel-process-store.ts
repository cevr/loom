import { AgentId, CodeKernelProcessRecord, ProcessIdentity, SessionId } from "@cvr/loom-domain";
import {
  CodeKernelProcessStore,
  CodeKernelProcessStoreError,
  type CodeKernelProcessStoreShape,
} from "@cvr/loom-runtime";
import { Effect, Layer, Schema } from "effect";
import { SqlClient, SqlSchema } from "effect/unstable/sql";
import type { SqlError } from "effect/unstable/sql/SqlError";

const ProcessRow = Schema.Struct({
  sessionId: SessionId,
  agentId: AgentId,
  ...ProcessIdentity.fields,
}).pipe(
  Schema.encodeKeys({
    sessionId: "session_id",
    agentId: "agent_id",
    processGroupId: "process_group_id",
    processStartId: "process_start_id",
  }),
);

const storeError = (operation: CodeKernelProcessStoreError["operation"]) =>
  Effect.mapError((cause: SqlError) => new CodeKernelProcessStoreError({ operation, cause }));

export const makeSqliteCodeKernelProcessStore: Effect.Effect<
  CodeKernelProcessStoreShape,
  never,
  SqlClient.SqlClient
> = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const list = SqlSchema.findAll({
    Request: Schema.Void,
    Result: ProcessRow,
    execute: () => sql`SELECT * FROM code_kernel_processes ORDER BY session_id, agent_id`,
  });
  const register = Effect.fn("SqliteCodeKernelProcessStore.register")(function* (
    record: CodeKernelProcessRecord,
  ) {
    const rows = yield* sql`
      INSERT INTO code_kernel_processes ${sql.insert({
        session_id: record.sessionId,
        agent_id: record.agentId,
        pid: record.pid,
        process_group_id: record.processGroupId,
        process_start_id: record.processStartId,
      })}
      ON CONFLICT(session_id, agent_id) DO NOTHING
      RETURNING session_id
    `.pipe(storeError("register"));
    return rows.length === 1;
  });
  const remove = Effect.fn("SqliteCodeKernelProcessStore.remove")(function* (
    record: CodeKernelProcessRecord,
  ) {
    const rows = yield* sql`
      DELETE FROM code_kernel_processes
      WHERE session_id = ${record.sessionId}
        AND agent_id = ${record.agentId}
        AND pid = ${record.pid}
        AND process_group_id = ${record.processGroupId}
        AND process_start_id = ${record.processStartId}
      RETURNING session_id
    `.pipe(storeError("remove"));
    return rows.length === 1;
  });
  return CodeKernelProcessStore.of({
    register,
    remove,
    list: list().pipe(Effect.catchTag("SchemaError", Effect.die), storeError("list")),
  });
});

export const layerSqliteCodeKernelProcessStore = Layer.effect(
  CodeKernelProcessStore,
  makeSqliteCodeKernelProcessStore,
);
