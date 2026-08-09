import { WorkspaceRoot } from "@cvr/loom-domain";
import { currentWorkingDirectory } from "@cvr/loom-platform-bun";
import { Config, Effect, FileSystem, Option, Path } from "effect";
import type { PlatformError } from "effect/PlatformError";

export interface DaemonConfig {
  readonly workspaceRoot: WorkspaceRoot;
  readonly socketPath: string;
  readonly databasePath: string;
}

const optionalPath = (name: string) => Config.option(Config.nonEmptyString(name));

export const loadDaemonConfig: Effect.Effect<
  DaemonConfig,
  Config.ConfigError | PlatformError,
  FileSystem.FileSystem | Path.Path
> = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const configuredRoot = yield* optionalPath("LOOM_WORKSPACE_ROOT");
  const rootInput = yield* Option.match(configuredRoot, {
    onNone: () => currentWorkingDirectory,
    onSome: Effect.succeed,
  });
  const root = yield* fs.realPath(path.resolve(rootInput));
  const workspaceRoot = WorkspaceRoot.make(root);
  const socketPath = Option.getOrElse(
    yield* optionalPath("LOOM_SOCKET_PATH"),
    () => `${workspaceRoot}/.loom/daemon.sock`,
  );
  const databasePath = Option.getOrElse(
    yield* optionalPath("LOOM_DB_PATH"),
    () => `${workspaceRoot}/.loom/loom.sqlite`,
  );
  return { workspaceRoot, socketPath, databasePath };
});
