import {
  processIdentitiesMatch,
  type CodeKernelProcessRecord,
  type ProcessIdentity,
} from "@cvr/loom-domain";
import { Effect, Schedule } from "effect";
import { CodeKernelProcessRecoveryError } from "./code-kernel-process-recovery-error.js";
import type { CodeKernelProcessStoreShape } from "./code-kernel-process-store.js";
import type { ProcessControllerShape } from "./process-controller.js";
import { ProcessObservation, type ProcessInspectorShape } from "./process-inspector.js";

export interface CodeKernelProcessRecoveryServices {
  readonly store: CodeKernelProcessStoreShape;
  readonly inspector: ProcessInspectorShape;
  readonly controller: ProcessControllerShape;
}

const recoveryError =
  (operation: CodeKernelProcessRecoveryError["operation"], record: CodeKernelProcessRecord) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.mapError(
      effect,
      (cause) => new CodeKernelProcessRecoveryError({ operation, record, cause }),
    );

const remove = (services: CodeKernelProcessRecoveryServices, record: CodeKernelProcessRecord) =>
  services.store.remove(record).pipe(recoveryError("remove", record), Effect.asVoid);

const awaitTermination = (
  services: CodeKernelProcessRecoveryServices,
  record: CodeKernelProcessRecord,
  identity: ProcessIdentity,
) =>
  services.controller.isGroupAlive(identity).pipe(
    Effect.repeat({
      while: (alive) => alive,
      schedule: Schedule.spaced("25 millis").pipe(Schedule.upTo({ times: 80 })),
    }),
    Effect.flatMap((alive) => {
      if (!alive) return Effect.void;
      return Effect.fail(
        new CodeKernelProcessRecoveryError({
          operation: "confirm",
          record,
          cause: "The Code Kernel process group did not terminate.",
        }),
      );
    }),
  );

const reconcileFound = (
  services: CodeKernelProcessRecoveryServices,
  record: CodeKernelProcessRecord,
  actual: ProcessIdentity,
) => {
  if (!processIdentitiesMatch(record, actual)) return remove(services, record);
  return services.controller
    .signalGroup(record, "SIGKILL")
    .pipe(
      recoveryError("terminate", record),
      Effect.andThen(awaitTermination(services, record, actual)),
      Effect.andThen(remove(services, record)),
    );
};

const reconcileRecord = Effect.fn("CodeKernelProcessRecovery.reconcileRecord")(function* (
  services: CodeKernelProcessRecoveryServices,
  record: CodeKernelProcessRecord,
) {
  const observation = yield* services.inspector
    .inspect(record.pid)
    .pipe(recoveryError("inspect", record));
  return yield* ProcessObservation.$match(observation, {
    Missing: () => remove(services, record),
    Found: ({ identity }) => reconcileFound(services, record, identity),
  });
});

export const reconcileCodeKernelProcesses = Effect.fn("CodeKernelProcessRecovery.reconcile")(
  function* (services: CodeKernelProcessRecoveryServices) {
    const records = yield* services.store.list;
    yield* Effect.forEach(
      records,
      (record) =>
        reconcileRecord(services, record).pipe(
          Effect.catch((error) =>
            Effect.logWarning("Code Kernel process reconciliation failed.", error),
          ),
        ),
      { concurrency: "unbounded", discard: true },
    );
  },
);
