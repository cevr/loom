import {
  JobId,
  JobProcessRecord,
  type JobProcessStatus,
  ProcessIdentity,
  SessionId,
} from "@cvr/loom-domain";
import { expect, it } from "effect-bun-test";
import { Effect, Ref } from "effect";
import {
  JobProcessStore,
  JobProcessStoreError,
  makeJobReconciler,
  ProcessInspectionError,
  ProcessInspector,
  ProcessObservation,
  type ProcessInspectorShape,
} from "../src/index.js";

const identity = ProcessIdentity.make({
  pid: 41001,
  processGroupId: 41001,
  processStartId: "Sun Aug  9 10:00:00 2026",
});

const record = JobProcessRecord.make({
  jobId: JobId.make("job-1"),
  sessionId: SessionId.make("session-1"),
  identity,
  stdoutPath: "/tmp/job-1.stdout",
  stderrPath: "/tmp/job-1.stderr",
  status: "Running",
  recoveryDetail: null,
});

const makeStore = Effect.fn("JobReconcilerTest.makeStore")(function* () {
  const current = yield* Ref.make(record);
  const service = JobProcessStore.of({
    initialize: Effect.void,
    upsert: (next) => Ref.set(current, next),
    listRecoverable: Ref.get(current).pipe(Effect.map((value) => [value])),
    updateRecovery: (jobId, status, recoveryDetail) =>
      Ref.update(current, (value) =>
        JobProcessRecord.make({ ...value, jobId, status, recoveryDetail }),
      ),
  });
  return { current, service };
});

const runRecovery = Effect.fn("JobReconcilerTest.runRecovery")(function* (
  inspector: ProcessInspectorShape,
) {
  const store = yield* makeStore();
  const reconciler = yield* makeJobReconciler.pipe(
    Effect.provideService(JobProcessStore, store.service),
    Effect.provideService(ProcessInspector, ProcessInspector.of(inspector)),
  );
  const results = yield* reconciler.reconcile;
  return { results, stored: yield* Ref.get(store.current) };
});

const expectStatus = (status: JobProcessStatus) => (stored: JobProcessRecord) => {
  expect(stored.status).toBe(status);
};

it.effect("recovers a process with the exact durable identity", () =>
  Effect.gen(function* () {
    const result = yield* runRecovery({
      inspect: () => Effect.succeed(ProcessObservation.Found({ identity })),
    });
    expect(result.results[0]).toHaveProperty("_tag", "Recovered");
    expectStatus("Recovered")(result.stored);
  }),
);

it.effect("marks a missing process as exited while offline", () =>
  Effect.gen(function* () {
    const result = yield* runRecovery({
      inspect: (pid) => Effect.succeed(ProcessObservation.Missing({ pid })),
    });
    expect(result.results[0]).toHaveProperty("_tag", "ExitedWhileOffline");
    expectStatus("ExitedWhileOffline")(result.stored);
  }),
);

it.effect("records an identity mismatch without adopting the process", () =>
  Effect.gen(function* () {
    const actual = ProcessIdentity.make({ ...identity, processStartId: "a later process" });
    const result = yield* runRecovery({
      inspect: () => Effect.succeed(ProcessObservation.Found({ identity: actual })),
    });
    expect(result.results[0]).toHaveProperty("_tag", "IdentityMismatch");
    expectStatus("IdentityMismatch")(result.stored);
  }),
);

it.effect("keeps the durable state recoverable when inspection fails", () =>
  Effect.gen(function* () {
    const result = yield* runRecovery({
      inspect: (pid) =>
        Effect.fail(new ProcessInspectionError({ pid, cause: "ps is unavailable" })),
    });
    expect(result.results[0]).toHaveProperty("_tag", "InspectionFailed");
    expectStatus("Running")(result.stored);
  }),
);

it.effect("propagates a durable state update failure", () =>
  Effect.gen(function* () {
    const store = yield* makeStore();
    const failure = new JobProcessStoreError({ operation: "updateRecovery", cause: "disk full" });
    const failingStore = JobProcessStore.of({
      ...store.service,
      updateRecovery: () => Effect.fail(failure),
    });
    const reconciler = yield* makeJobReconciler.pipe(
      Effect.provideService(JobProcessStore, failingStore),
      Effect.provideService(
        ProcessInspector,
        ProcessInspector.of({
          inspect: () => Effect.succeed(ProcessObservation.Found({ identity })),
        }),
      ),
    );

    const error = yield* reconciler.reconcile.pipe(Effect.flip);
    expect(error).toBe(failure);
  }),
);
