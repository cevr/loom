/* oxlint-disable effect/noGlobals -- This host adapter test inspects the current Bun process. */
import { BunServices } from "@effect/platform-bun";
import { SqliteClient } from "@effect/sql-sqlite-bun";
import { JobId, JobProcessRecord, ProcessIdentity, SessionId } from "@cvr/loom-domain";
import { JobProcessStore, ProcessObservation } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer } from "effect";
import { layerSqliteJobProcessStore, makeBunProcessInspector } from "../src/index.js";

const record = JobProcessRecord.make({
  jobId: JobId.make("job-persistent"),
  sessionId: SessionId.make("session-1"),
  identity: ProcessIdentity.make({
    pid: 42001,
    processGroupId: 42001,
    processStartId: "Sun Aug  9 10:00:00 2026",
  }),
  stdoutPath: "/tmp/job-persistent.stdout",
  stderrPath: "/tmp/job-persistent.stderr",
  status: "Running",
  recoveryDetail: null,
});

const withStore = <A, E>(filename: string, effect: Effect.Effect<A, E, JobProcessStore>) =>
  Effect.scoped(
    effect.pipe(
      Effect.provide(
        layerSqliteJobProcessStore.pipe(Layer.provide(SqliteClient.layer({ filename }))),
      ),
    ),
  );

it.scoped("persists a recoverable process across SQLite client restarts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-recovery-" });
    const filename = `${directory}/loom.sqlite`;

    yield* withStore(
      filename,
      Effect.gen(function* () {
        const store = yield* JobProcessStore;
        yield* store.initialize;
        yield* store.upsert(record);
      }),
    );

    const recovered = yield* withStore(
      filename,
      Effect.gen(function* () {
        const store = yield* JobProcessStore;
        yield* store.initialize;
        return yield* store.listRecoverable;
      }),
    );

    expect(recovered).toEqual([record]);
  }).pipe(Effect.provide(BunServices.layer)),
);

it.effect("reads the stable identity of a live process", () =>
  Effect.gen(function* () {
    const inspector = yield* makeBunProcessInspector;
    const observation = yield* inspector.inspect(process.pid);
    yield* ProcessObservation.$match(observation, {
      Missing: () => Effect.die("The current process was not present in the process table."),
      Found: ({ identity }) =>
        Effect.sync(() => {
          expect(identity.pid).toBe(process.pid);
          expect(identity.processGroupId).toBeGreaterThan(0);
          expect(identity.processStartId.length).toBeGreaterThan(0);
        }),
    });
  }).pipe(Effect.provide(BunServices.layer)),
);
