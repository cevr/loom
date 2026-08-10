/* oxlint-disable effect/noNullish -- BunSocket.runRaw and Deferred<void> require the JavaScript void value. */
import { BunSocket } from "@effect/platform-bun";
import { Deferred, Effect, Fiber, FileSystem, Path } from "effect";
import type { PlatformError } from "effect/PlatformError";
import { DaemonAlreadyRunningError } from "./daemon-already-running-error.js";

const isLiveSocket = (socketPath: string): Effect.Effect<boolean> =>
  Effect.scoped(
    Effect.gen(function* () {
      const socket = yield* BunSocket.makeNet({ path: socketPath, openTimeout: "250 millis" });
      const opened = yield* Deferred.make<void>();
      const reader = yield* socket
        .runRaw(() => undefined, { onOpen: Deferred.succeed(opened, undefined) })
        .pipe(Effect.forkScoped);
      return yield* Effect.raceFirst(
        Deferred.await(opened).pipe(Effect.as(true)),
        Fiber.await(reader).pipe(Effect.as(false)),
      );
    }),
  );

export const prepareDaemonSocket = Effect.fn("BunDaemonSocket.prepare")(function* (
  socketPath: string,
): Effect.fn.Return<
  void,
  DaemonAlreadyRunningError | PlatformError,
  FileSystem.FileSystem | Path.Path
> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  yield* fs.makeDirectory(path.dirname(socketPath), { recursive: true });
  if (!(yield* fs.exists(socketPath))) return;
  if (yield* isLiveSocket(socketPath)) {
    return yield* new DaemonAlreadyRunningError({ socketPath });
  }
  yield* fs.remove(socketPath);
});
