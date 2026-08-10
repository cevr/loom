import { type JobId, type JobProcessRecord, type ProcessIdentity } from "@cvr/loom-domain";
import {
  Context,
  Data,
  Effect,
  FiberMap,
  Inspectable,
  Layer,
  Option,
  Schedule,
  Scope,
} from "effect";
import type { JobProcessStoreError } from "./job-process-store-error.js";
import { JobProcessStore, type JobProcessStoreShape } from "./job-process-store.js";
import {
  ProcessInspector,
  ProcessObservation,
  type ProcessInspectorShape,
} from "./process-inspector.js";
import type { WorkflowCapabilityStoreError } from "./workflow-capability-store-error.js";
import { WorkflowJobStore } from "./workflow-job-store.js";

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
  readonly reconcile: Effect.Effect<
    ReadonlyArray<JobRecoveryResult>,
    JobProcessStoreError | WorkflowCapabilityStoreError
  >;
  readonly watch: (record: JobProcessRecord) => Effect.Effect<void>;
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

const markExited = (store: JobProcessStoreShape, record: JobProcessRecord) =>
  store.updateRecovery(record.jobId, "Exited", Option.none()).pipe(Effect.as(false));

const markAbandoned = (store: JobProcessStoreShape, record: JobProcessRecord) =>
  store
    .updateRecovery(
      record.jobId,
      "ExitedWhileOffline",
      Option.some("The Job launch did not commit before the daemon stopped."),
    )
    .pipe(Effect.as(JobRecoveryResult.ExitedWhileOffline({ jobId: record.jobId })));

const identitiesMatch = (expected: ProcessIdentity, actual: ProcessIdentity): boolean =>
  expected.pid === actual.pid &&
  expected.processGroupId === actual.processGroupId &&
  expected.processStartId === actual.processStartId;

const mismatchDetail = (expected: ProcessIdentity, actual: ProcessIdentity): string =>
  `Expected PID ${expected.pid}, PGID ${expected.processGroupId}, start ${expected.processStartId}; ` +
  `found PID ${actual.pid}, PGID ${actual.processGroupId}, start ${actual.processStartId}`;

const monitorPass = Effect.fn("JobReconciler.monitorPass")(function* (
  store: JobProcessStoreShape,
  inspector: ProcessInspectorShape,
  record: JobProcessRecord,
) {
  const observation = yield* inspector.inspect(record.identity.pid);
  return yield* ProcessObservation.$match(observation, {
    Missing: () => markExited(store, record),
    Found: ({ identity }) => {
      if (identitiesMatch(record.identity, identity)) return Effect.succeed(true);
      return store
        .updateRecovery(
          record.jobId,
          "IdentityMismatch",
          Option.some(mismatchDetail(record.identity, identity)),
        )
        .pipe(Effect.as(false));
    },
  });
});

const makeWatch = (
  store: JobProcessStoreShape,
  inspector: ProcessInspectorShape,
  monitors: FiberMap.FiberMap<JobId>,
) =>
  Effect.fn("JobReconciler.watch")(function* (record: JobProcessRecord) {
    const monitor = monitorPass(store, inspector, record).pipe(
      Effect.catch((error) =>
        Effect.logWarning(
          `Job process monitor pass failed for ${record.jobId}.`,
          Inspectable.toStringUnknown(error),
        ).pipe(Effect.as(true)),
      ),
      Effect.repeat({ while: (isRunning) => isRunning, schedule: Schedule.spaced("250 millis") }),
      Effect.asVoid,
    );
    yield* FiberMap.run(monitors, record.jobId, monitor, { onlyIfMissing: true });
  });

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
  JobProcessStore | ProcessInspector | Scope.Scope | WorkflowJobStore
> = Effect.gen(function* () {
  const store = yield* JobProcessStore;
  const inspector = yield* ProcessInspector;
  const jobs = yield* WorkflowJobStore;
  const monitors = yield* FiberMap.make<JobId>();
  const watch = makeWatch(store, inspector, monitors);

  const reconcileOne = Effect.fn("JobReconciler.reconcileOne")(function* (
    record: JobProcessRecord,
  ) {
    const result = yield* Effect.matchEffect(inspector.inspect(record.identity.pid), {
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
    if (JobRecoveryResult.$is("Recovered")(result)) yield* watch(record);
    return result;
  });

  const reconcile = Effect.fn("JobReconciler.reconcile")(function* () {
    const abandoned = new Set(yield* jobs.failStarting);
    const records = yield* store.listRecoverable;
    return yield* Effect.forEach(records, (record) => {
      if (abandoned.has(record.jobId)) return markAbandoned(store, record);
      return reconcileOne(record);
    });
  });

  return JobReconciler.of({ reconcile: reconcile(), watch });
});

export const layerJobReconciler: Layer.Layer<
  JobReconciler,
  never,
  JobProcessStore | ProcessInspector | WorkflowJobStore
> = Layer.effect(JobReconciler, makeJobReconciler);
