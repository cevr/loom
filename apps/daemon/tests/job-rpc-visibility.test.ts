import { BunServices } from "@effect/platform-bun";
import { JobId, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { expect, it } from "effect-bun-test";
import { Effect, Fiber, FileSystem } from "effect";
import { runLoomDaemon } from "../src/program.js";
import { testCapabilities, withClient } from "./workflow-test-support.js";

const scopedLive = it.scopedLive.layer(BunServices.layer);

scopedLive("keeps a fast terminal Job visible after Start returns", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-visibility-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const socketPath = `${directory}/daemon.sock`;
    const daemon = yield* runLoomDaemon(
      { workspaceRoot, socketPath, databasePath: `${directory}/loom.sqlite` },
      testCapabilities({
        supports: () => false,
        execute: () => Effect.die("This test does not run a Workflow capability."),
        compensate: () => Effect.void,
      }),
    ).pipe(Effect.forkScoped);
    const sessionId = SessionId.make("fast-job-visibility");

    yield* withClient(workspaceRoot, socketPath, (client) =>
      Effect.forEach(
        Array.from({ length: 20 }, (_, index) => index),
        (index) =>
          Effect.gen(function* () {
            const marker = `fast-job-${index}`;
            const jobId = JobId.make(marker);
            const address = { sessionId, jobId };
            const started = yield* client.startJob({
              ...address,
              command: `printf '${marker}'`,
              attached: true,
              foregroundLeaseMillis: 5_000,
            });
            const awaited = yield* client.awaitJob({ ...address, foregroundLeaseMillis: 5_000 });
            const inspected = yield* client.inspectJob(address);
            const output = yield* client.readJobOutput({
              ...address,
              stream: "stdout",
              sequence: 0,
              maximumBytes: 128,
            });

            expect(started.status).toBe("Succeeded");
            expect(awaited).toEqual(started);
            expect(inspected).toEqual(started);
            expect(new TextDecoder().decode(output.data)).toBe(marker);
            expect(output.complete).toBe(true);
          }),
      ),
    );

    yield* Fiber.interrupt(daemon);
  }),
);
