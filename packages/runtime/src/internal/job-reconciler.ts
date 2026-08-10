import { type JobId, type JobProcessRecord, type ProcessIdentity } from "@cvr/loom-domain";
import { Context, Data, Effect, Layer, Option } from "effect";
import type { JobProcessStoreError } from "./job-process-store-error.js";
import { JobProcessStore, type JobProcessStoreShape } from "./job-process-store.js";
import { ProcessInspector, ProcessObservation } from "./process-inspector.js";

export type JobRecoveryResult = Data.TaggedEnum<{
  Recovered: {
    readonly jobId: JobId;
    readonly identity: ProcessIdentity;
  };
  ExitedWhileOffline: { readonly jobId: JobId };
  IdentityMismatch: {
    readonly jobId: JobId;
    readonly expected: ProcessIdentity;
    readonly actual: ProcessIdentity;
  };
  InspectionFailed: {
    readonly jobId: JobId;
    readonly message: string;
  };
}>;

export const JobRecoveryResult = Data.taggedEnum<JobRecoveryResult>();

export interface JobReconcilerShape {
  readonly reconcile: Effect.Effect<ReadonlyArray<JobRecoveryResult>, JobProcessStoreError>;
}

export class JobReconciler extends Context.Service<JobReconciler, JobReconcilerShape>()(
  "@cvr/loom-runtime/JobReconciler",
) {}

const markMissing = (store: JobProcessStoreShape, record: JobProcessRecord) =>
  store
    .updateRecovery(
      record.jobId,
      "ExitedWhileOffline",
      Option.some("The recorded process no longer exists."),
    )
    .pipe(Effect.as(JobRecoveryResult.ExitedWhileOffline({ jobId: record.jobId })));

const identitiesMatch = (expected: ProcessIdentity, actual: ProcessIdentity): boolean =>
  expected.pid === actual.pid &&
  expected.processGroupId === actual.processGroupId &&
  expected.processStartId === actual.processStartId;

const mismatchDetail = (expected: ProcessIdentity, actual: ProcessIdentity): string =>
  `Expected PID ${expected.pid}, PGID ${expected.processGroupId}, start ${expected.processStartId}; ` +
  `found PID ${actual.pid}, PGID ${actual.processGroupId}, start ${actual.processStartId}`;

const markFound = (
  store: JobProcessStoreShape,
  record: JobProcessRecord,
  identity: ProcessIdentity,
) => {
  if (identitiesMatch(record.identity, identity)) {
    return store
      .updateRecovery(record.jobId, "Recovered", Option.none())
      .pipe(Effect.as(JobRecoveryResult.Recovered({ jobId: record.jobId, identity })));
  }
  return store
    .updateRecovery(
      record.jobId,
      "IdentityMismatch",
      Option.some(mismatchDetail(record.identity, identity)),
    )
    .pipe(
      Effect.as(
        JobRecoveryResult.IdentityMismatch({
          jobId: record.jobId,
          expected: record.identity,
          actual: identity,
        }),
      ),
    );
};

export const makeJobReconciler: Effect.Effect<
  JobReconcilerShape,
  never,
  JobProcessStore | ProcessInspector
> = Effect.gen(function* () {
  const store = yield* JobProcessStore;
  const inspector = yield* ProcessInspector;

  const reconcileOne = Effect.fn("JobReconciler.reconcileOne")(function* (
    record: JobProcessRecord,
  ) {
    return yield* Effect.matchEffect(inspector.inspect(record.identity.pid), {
      onFailure: (error) =>
        Effect.succeed(
          JobRecoveryResult.InspectionFailed({ jobId: record.jobId, message: error.message }),
        ),
      onSuccess: (observation) =>
        ProcessObservation.$match(observation, {
          Missing: () => markMissing(store, record),
          Found: ({ identity }) => markFound(store, record, identity),
        }),
    });
  });

  const reconcile = Effect.fn("JobReconciler.reconcile")(function* () {
    const records = yield* store.listRecoverable;
    return yield* Effect.forEach(records, reconcileOne);
  });

  return JobReconciler.of({ reconcile: reconcile() });
});

export const layerJobReconciler: Layer.Layer<
  JobReconciler,
  never,
  JobProcessStore | ProcessInspector
> = Layer.effect(JobReconciler, makeJobReconciler);
