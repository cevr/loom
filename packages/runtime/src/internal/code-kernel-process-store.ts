import type { CodeKernelProcessRecord } from "@cvr/loom-domain";
import { Context, type Effect } from "effect";
import type { CodeKernelProcessStoreError } from "./code-kernel-process-store-error.js";

export interface CodeKernelProcessStoreShape {
  readonly register: (
    record: CodeKernelProcessRecord,
  ) => Effect.Effect<boolean, CodeKernelProcessStoreError>;
  readonly remove: (
    record: CodeKernelProcessRecord,
  ) => Effect.Effect<boolean, CodeKernelProcessStoreError>;
  readonly list: Effect.Effect<ReadonlyArray<CodeKernelProcessRecord>, CodeKernelProcessStoreError>;
}

export class CodeKernelProcessStore extends Context.Service<
  CodeKernelProcessStore,
  CodeKernelProcessStoreShape
>()("@cvr/loom-runtime/CodeKernelProcessStore") {}
