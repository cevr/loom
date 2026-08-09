/* oxlint-disable effect/noGlobals -- This named Bun adapter owns detached process start. */
import type { WorkspaceRoot } from "@cvr/loom-domain";
import { Effect, FileSystem } from "effect";
import type { PlatformError } from "effect/PlatformError";
import { DaemonStartError } from "./daemon-start-error.js";

export interface StartBunDaemonConfig {
  readonly entryPath: string;
  readonly workspaceRoot: WorkspaceRoot;
}

export const startBunDaemon = Effect.fn("BunDaemon.start")(function* (
  config: StartBunDaemonConfig,
): Effect.fn.Return<void, DaemonStartError | PlatformError, FileSystem.FileSystem> {
  const fs = yield* FileSystem.FileSystem;
  const stateDirectory = `${config.workspaceRoot}/.loom`;
  yield* fs.makeDirectory(stateDirectory, { recursive: true });
  const child = yield* Effect.try({
    try: () =>
      Bun.spawn({
        cmd: [process.execPath, "run", config.entryPath],
        cwd: config.workspaceRoot,
        detached: true,
        stdin: "ignore",
        stdout: Bun.file(`${stateDirectory}/daemon.stdout.log`),
        stderr: Bun.file(`${stateDirectory}/daemon.stderr.log`),
      }),
    catch: (cause) =>
      new DaemonStartError({
        entryPath: config.entryPath,
        workspaceRoot: config.workspaceRoot,
        cause,
      }),
  });
  child.unref();
});
