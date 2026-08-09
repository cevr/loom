import type { ProcessIdentity } from "@cvr/loom-domain";
import { Context, Data, type Effect } from "effect";
import type { ProcessInspectionError } from "./process-inspection-error.js";

export type ProcessObservation = Data.TaggedEnum<{
  Missing: { readonly pid: number };
  Found: { readonly identity: ProcessIdentity };
}>;

export const ProcessObservation = Data.taggedEnum<ProcessObservation>();

export interface ProcessInspectorShape {
  readonly inspect: (pid: number) => Effect.Effect<ProcessObservation, ProcessInspectionError>;
}

export class ProcessInspector extends Context.Service<ProcessInspector, ProcessInspectorShape>()(
  "@cvr/loom-runtime/ProcessInspector",
) {}
