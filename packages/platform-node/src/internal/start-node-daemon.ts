import type { WorkspaceRoot } from "@cvr/loom-domain";
import { Effect, FileSystem } from "effect";
import type { PlatformError } from "effect/PlatformError";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

export interface StartNodeDaemonConfig {
  readonly entryPath: string;
  readonly workspaceRoot: WorkspaceRoot;
}

export const startNodeDaemon = Effect.fn("NodeDaemon.start")(function* (
  config: StartNodeDaemonConfig,
): Effect.fn.Return<
  void,
  PlatformError,
  FileSystem.FileSystem | ChildProcessSpawner.ChildProcessSpawner
> {
  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(`${config.workspaceRoot}/.loom`, { recursive: true });
  yield* Effect.scoped(
    Effect.gen(function* () {
      const handle = yield* ChildProcess.make("bun", ["run", config.entryPath], {
        cwd: config.workspaceRoot,
        detached: true,
        stdin: "ignore",
        stdout: "ignore",
        stderr: "ignore",
      });
      yield* Effect.asVoid(handle.unref);
    }),
  );
});
