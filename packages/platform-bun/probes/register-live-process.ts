import { BunRuntime, BunServices } from "@effect/platform-bun";
import { JobId, JobProcessRecord, SessionId } from "@cvr/loom-domain";
import { JobProcessStore, ProcessInspector, ProcessObservation } from "@cvr/loom-runtime";
import { Config, Effect, Layer, Schema } from "effect";
import {
  layerBunProcessInspector,
  layerLoomSqlite,
  layerSqliteJobProcessStore,
} from "../src/index.js";

const platformLayer = (filename: string) =>
  Layer.merge(
    layerBunProcessInspector,
    layerSqliteJobProcessStore.pipe(Layer.provide(layerLoomSqlite({ filename }))),
  ).pipe(Layer.provide(BunServices.layer));

const encodeRecord = Schema.encodeSync(Schema.fromJsonString(JobProcessRecord));

const program = Effect.gen(function* () {
  const pid = yield* Config.int("LOOM_PROBE_PID");
  const filename = yield* Config.string("LOOM_PROBE_DB");

  yield* Effect.gen(function* () {
    const inspector = yield* ProcessInspector;
    const store = yield* JobProcessStore;
    const observation = yield* inspector.inspect(pid);
    const identity = yield* ProcessObservation.$match(observation, {
      Missing: () => Effect.fail(`PID ${pid} is not live.`),
      Found: ({ identity: foundIdentity }) => Effect.succeed(foundIdentity),
    });
    const record = JobProcessRecord.make({
      jobId: JobId.make("pi-live-probe"),
      sessionId: SessionId.make("pi-live-session"),
      identity,
      stdoutPath: "/tmp/loom-pi-live.stdout",
      stderrPath: "/tmp/loom-pi-live.stderr",
      status: "Running",
      recoveryDetail: null,
    });
    yield* store.upsert(record);
    yield* Effect.log(encodeRecord(record));
  }).pipe(Effect.provide(platformLayer(filename)));
});

BunRuntime.runMain(program);
