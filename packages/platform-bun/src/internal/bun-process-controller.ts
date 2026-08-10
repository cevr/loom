/* oxlint-disable effect/noGlobals -- This Bun adapter controls operating-system process groups. */
import {
  ProcessController,
  ProcessControllerError,
  type ProcessControllerShape,
} from "@cvr/loom-runtime";
import { Effect, Layer } from "effect";

export const makeBunProcessController: ProcessControllerShape = ProcessController.of({
  isGroupAlive: (identity) =>
    Effect.try(() => process.kill(-identity.processGroupId, 0)).pipe(
      Effect.match({ onFailure: () => false, onSuccess: () => true }),
    ),
  signalGroup: (identity, signal) =>
    Effect.try({
      try: () => process.kill(-identity.processGroupId, signal),
      catch: (cause) =>
        new ProcessControllerError({
          processGroupId: identity.processGroupId,
          signal,
          cause,
        }),
    }).pipe(Effect.asVoid),
});

export const layerBunProcessController: Layer.Layer<ProcessController> = Layer.succeed(
  ProcessController,
  makeBunProcessController,
);
