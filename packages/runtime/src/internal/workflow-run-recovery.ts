import type { WorkflowRunAddress } from "@cvr/loom-domain";
import type { WorkflowRunAcceptanceError } from "@cvr/loom-protocol";
import { Context, Effect, Layer } from "effect";
import { WorkflowRunAcceptance } from "./workflow-run-acceptance.js";
import { WorkflowRunStatePublisher } from "./workflow-run-state-publisher.js";

export interface WorkflowRunRecoveryShape {
  readonly retire: Effect.Effect<void, WorkflowRunAcceptanceError>;
  readonly recover: Effect.Effect<void, WorkflowRunAcceptanceError>;
}

export class WorkflowRunRecovery extends Context.Service<
  WorkflowRunRecovery,
  WorkflowRunRecoveryShape
>()("@cvr/loom-runtime/WorkflowRunRecovery") {}

const forEachAccepted = (
  acceptance: WorkflowRunAcceptance["Service"],
  operation: (address: WorkflowRunAddress) => Effect.Effect<void>,
) =>
  acceptance.list.pipe(
    Effect.flatMap((addresses) =>
      Effect.forEach(addresses, operation, { concurrency: "unbounded", discard: true }),
    ),
  );

export const makeWorkflowRunRecovery = Effect.gen(function* () {
  const acceptance = yield* WorkflowRunAcceptance;
  const publisher = yield* WorkflowRunStatePublisher;
  return WorkflowRunRecovery.of({
    retire: forEachAccepted(acceptance, publisher.retire),
    recover: forEachAccepted(acceptance, publisher.recover),
  });
});

export const layerWorkflowRunRecovery = Layer.effect(WorkflowRunRecovery, makeWorkflowRunRecovery);
