import type { ProcessIdentity } from "@cvr/loom-domain";
import { Context, type Effect } from "effect";
import type { ProcessControllerError } from "./process-controller-error.js";

export type ProcessSignal = "SIGTERM" | "SIGKILL";

export interface ProcessControllerShape {
  readonly isGroupAlive: (identity: ProcessIdentity) => Effect.Effect<boolean>;
  readonly signalGroup: (
    identity: ProcessIdentity,
    signal: ProcessSignal,
  ) => Effect.Effect<void, ProcessControllerError>;
}

export class ProcessController extends Context.Service<ProcessController, ProcessControllerShape>()(
  "@cvr/loom-runtime/ProcessController",
) {}
