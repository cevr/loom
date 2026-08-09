import { CodeKernelDiagnostic } from "@cvr/loom-protocol";
import { Clock, Effect, FileSystem, Ref, Stream } from "effect";
import type { ChildProcessHandle } from "effect/unstable/process/ChildProcessSpawner";
import { CodeKernelProcessError } from "./code-kernel-process-error.js";

export interface CodeKernelDiagnosticsConfig {
  readonly stderrTailCharacters?: number;
  readonly diagnosticsDirectory?: string;
  readonly diagnosticFileLimit?: number;
}

export interface KernelDiagnosticSource {
  readonly stderrTail: Ref.Ref<string>;
  readonly stderrPath: string | undefined;
}

export const prepareDiagnosticPath = Effect.fn("CodeKernelDiagnostics.preparePath")(function* (
  config: CodeKernelDiagnosticsConfig,
  fs: FileSystem.FileSystem,
  pid: number,
) {
  if (config.diagnosticsDirectory === undefined) return undefined;
  yield* fs.makeDirectory(config.diagnosticsDirectory, { recursive: true });
  const entries = yield* fs.readDirectory(config.diagnosticsDirectory);
  const keep = (config.diagnosticFileLimit ?? 20) - 1;
  const existing = entries.filter((entry) => entry.endsWith(".stderr.log")).toSorted();
  const stale = existing.slice(0, Math.max(0, existing.length - keep));
  yield* Effect.forEach(stale, (entry) =>
    fs.remove(`${config.diagnosticsDirectory}/${entry}`, { force: true }),
  );
  const now = yield* Clock.currentTimeMillis;
  return `${config.diagnosticsDirectory}/${now}-${pid}.stderr.log`;
});

export const captureKernelStderr = (
  config: CodeKernelDiagnosticsConfig,
  handle: ChildProcessHandle,
  source: KernelDiagnosticSource,
  fs: FileSystem.FileSystem,
) => {
  const maximum = config.stderrTailCharacters ?? 64 * 1024;
  const decoded = handle.stderr.pipe(
    Stream.decodeText(),
    Stream.tap((chunk) =>
      Ref.update(source.stderrTail, (current) => `${current}${chunk}`.slice(-maximum)),
    ),
  );
  if (source.stderrPath === undefined) return Stream.runDrain(decoded);
  return decoded.pipe(Stream.encodeText, Stream.run(fs.sink(source.stderrPath)));
};

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
    stderrPath: source.stderrPath,
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
