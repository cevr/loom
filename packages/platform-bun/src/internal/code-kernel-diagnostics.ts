import { CodeKernelDiagnostic } from "@cvr/loom-protocol";
import { Effect, FileSystem, Ref, Stream } from "effect";
import type { ChildProcessHandle } from "effect/unstable/process/ChildProcessSpawner";
import type { KernelDiagnosticFile } from "./code-kernel-diagnostic-store.js";
import { CodeKernelProcessError } from "./code-kernel-process-error.js";

export interface CodeKernelDiagnosticsConfig {
  readonly stderrTailCharacters?: number;
}

export interface KernelDiagnosticSource {
  readonly stderrTail: Ref.Ref<string>;
  readonly file: KernelDiagnosticFile | undefined;
}

const truncationMarker = new TextEncoder().encode("\n[loom: stderr truncated]\n");

const appendDiagnosticTail = Effect.fn("CodeKernelDiagnostics.appendTail")(function* (
  source: KernelDiagnosticSource,
  fs: FileSystem.FileSystem,
) {
  if (source.file === undefined) return;
  const maximum = source.file.maxFileBytes;
  const tail = new TextEncoder().encode(yield* Ref.get(source.stderrTail));
  const tailBudget = Math.max(0, maximum - truncationMarker.length);
  const retainedTail = tail.subarray(Math.max(0, tail.length - tailBudget));
  const marker = truncationMarker.subarray(0, Math.max(0, maximum - retainedTail.length));
  const trailer = new Uint8Array(marker.length + retainedTail.length);
  trailer.set(marker);
  trailer.set(retainedTail, marker.length);
  yield* fs.truncate(source.file.path, maximum - trailer.length);
  yield* fs.writeFile(source.file.path, trailer, { flag: "a" });
});

export const captureKernelStderr = Effect.fn("CodeKernelDiagnostics.captureStderr")(function* (
  config: CodeKernelDiagnosticsConfig,
  handle: ChildProcessHandle,
  source: KernelDiagnosticSource,
  fs: FileSystem.FileSystem,
) {
  const maximum = Math.max(0, Math.floor(config.stderrTailCharacters ?? 64 * 1024));
  const decoded = handle.stderr.pipe(
    Stream.decodeText(),
    Stream.tap((chunk) =>
      Ref.update(source.stderrTail, (current) => {
        if (maximum === 0) return "";
        return `${current}${chunk}`.slice(-maximum);
      }),
    ),
  );
  const file = source.file;
  if (file === undefined) return yield* Stream.runDrain(decoded);
  const truncated = yield* Ref.make(false);
  const fileAvailable = yield* Ref.make(true);
  const written = yield* Ref.make(0);
  yield* decoded.pipe(
    Stream.encodeText,
    Stream.runForEach((chunk) =>
      Effect.gen(function* () {
        if (!(yield* Ref.get(fileAvailable))) return;
        const current = yield* Ref.get(written);
        const remaining = Math.max(0, file.maxFileBytes - current);
        if (chunk.length > remaining) yield* Ref.set(truncated, true);
        if (remaining === 0) return;
        const retained = chunk.subarray(0, remaining);
        yield* fs.writeFile(file.path, retained, { flag: "a" }).pipe(
          Effect.tapError((error) =>
            Effect.logWarning("Code Kernel stderr file write failed.", error),
          ),
          Effect.matchEffect({
            onFailure: () => Ref.set(fileAvailable, false),
            onSuccess: () => Ref.update(written, (total) => total + retained.length),
          }),
        );
      }),
    ),
  );
  if ((yield* Ref.get(fileAvailable)) && (yield* Ref.get(truncated))) {
    yield* appendDiagnosticTail(source, fs);
  }
});

export const diagnosticFor = Effect.fn("CodeKernelDiagnostics.current")(function* (
  source: KernelDiagnosticSource,
  options?: { readonly requestId?: number; readonly exitCode?: number },
) {
  const stderrTail = yield* Ref.get(source.stderrTail);
  let retainedStderr: string | undefined;
  if (stderrTail.length > 0) retainedStderr = stderrTail;
  return CodeKernelDiagnostic.make({
    requestId: options?.requestId,
    exitCode: options?.exitCode,
    stderrTail: retainedStderr,
    stderrPath: source.file?.path,
  });
});

export const failWithDiagnostic = (
  source: KernelDiagnosticSource,
  reason: CodeKernelProcessError["reason"],
  message: string,
  cause: unknown,
  requestId?: number,
) =>
  diagnosticFor(source, { requestId }).pipe(
    Effect.flatMap((diagnostic) =>
      Effect.fail(new CodeKernelProcessError({ reason, message, cause, diagnostic })),
    ),
  );
