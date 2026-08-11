import type { WorkflowRunAddress } from "@cvr/loom-domain";
import type { WorkflowRunAcceptanceError } from "@cvr/loom-protocol";
import { Context, Effect, Layer } from "effect";
import { WorkflowRunAcceptance } from "./workflow-run-acceptance.js";
import { WorkflowRunRetention } from "./workflow-run-retention.js";
import type { WorkflowRunRetentionError } from "./workflow-run-retention-error.js";
import { WorkflowRunStatePublisher } from "./workflow-run-state-publisher.js";

export interface WorkflowRunRecoveryShape {
  readonly retire: Effect.Effect<void, WorkflowRunAcceptanceError | WorkflowRunRetentionError>;
  readonly recover: Effect.Effect<void, WorkflowRunAcceptanceError>;
}

export class WorkflowRunRecovery extends Context.Service<
  WorkflowRunRecovery,
  WorkflowRunRecoveryShape
>()("@cvr/loom-runtime/WorkflowRunRecovery") {}

const forEachAccepted = <E>(
  addresses: Effect.Effect<ReadonlyArray<WorkflowRunAddress>, WorkflowRunAcceptanceError>,
  operation: (address: WorkflowRunAddress) => Effect.Effect<void, E>,
) =>
  addresses.pipe(
    Effect.flatMap((accepted) =>
      Effect.forEach(accepted, operation, { concurrency: "unbounded", discard: true }),
    ),
  );

export const makeWorkflowRunRecovery = Effect.gen(function* () {
  const acceptance = yield* WorkflowRunAcceptance;
  const publisher = yield* WorkflowRunStatePublisher;
  const retention = yield* WorkflowRunRetention;
  return WorkflowRunRecovery.of({
    retire: forEachAccepted(acceptance.listRetiring, retention.resumeRetirement).pipe(
      Effect.andThen(forEachAccepted(acceptance.listActive, publisher.retire)),
    ),
    recover: forEachAccepted(acceptance.listActive, publisher.watch),
  });
});

export const layerWorkflowRunRecovery = Layer.effect(WorkflowRunRecovery, makeWorkflowRunRecovery);
