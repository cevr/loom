import { Schema } from "effect";

export class CodeKernelProcessStoreError extends Schema.TaggedError<CodeKernelProcessStoreError>()(
  "CodeKernelProcessStoreError",
  {
    operation: Schema.Literals(["register", "remove", "list"]),
    cause: Schema.Defect(),
  },
) {}
