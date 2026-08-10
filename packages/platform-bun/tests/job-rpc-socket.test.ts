import { BunServices } from "@effect/platform-bun";
import { LoomClient } from "@cvr/loom-client";
import { JobId, SessionId, WorkspaceRoot } from "@cvr/loom-domain";
import { LoomRpcs } from "@cvr/loom-protocol";
import { makeConnectionHandshake } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { type Duration, Effect, FileSystem, Layer } from "effect";
import { layerBunLoomClient, layerBunLoomServer } from "../src/index.js";
import { makeStubJobHandlers } from "./job-rpc-test-support.js";

const sessionId = SessionId.make("job-socket-session");
const workspaceRoot = WorkspaceRoot.make("/workspace");

const layerHandlers = (startDelay: Duration.Input) =>
  LoomRpcs.toLayer(
    Effect.sync(() => {
      const jobs = makeStubJobHandlers(sessionId);
      const unused = () => Effect.die("This RPC is not used by the Job socket test.");
      return LoomRpcs.of({
        ...jobs,
        "Job.Start": (request) =>
          Effect.sleep(startDelay).pipe(Effect.andThen(jobs["Job.Start"](request))),
        "Connection.Handshake": makeConnectionHandshake({
          workspaceRoot,
          daemonStartedAtMillis: 100,
        }).handshake,
        "Session.Close": unused,
        "CodeKernel.EvaluateCell": unused,
        "CodeKernel.Reset": unused,
        "Workflow.Execute": unused,
        "Workflow.Signal": unused,
        "Workflow.Start": unused,
        "Workflow.Inspect": unused,
        "Workflow.Interrupt": unused,
        "Workflow.DecideCompensation": unused,
      });
    }),
  );

const layerLive = (
  socketPath: string,
  startDelay: Duration.Input,
  requestTimeout: Duration.Input,
) =>
  Layer.merge(
    layerBunLoomClient({ socketPath, workspaceRoot, requestTimeout }),
    layerBunLoomServer({ socketPath }).pipe(Layer.provide(layerHandlers(startDelay))),
  );

const scopedLive = it.scopedLive.layer(BunServices.layer);

scopedLive("round-trips every Job operation through a Unix socket", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-socket-" });
    const live = layerLive(`${directory}/daemon.sock`, 0, "2 seconds");

    yield* Effect.gen(function* () {
      const client = yield* LoomClient;
      const jobId = JobId.make("job-1");
      const address = { sessionId, jobId };
      const job = yield* client.startJob({
        ...address,
        command: "exit 0",
        attached: true,
        foregroundLeaseMillis: 0,
      });
      yield* client.inspectJob(address);
      yield* client.awaitJob({ ...address, foregroundLeaseMillis: 0 });
      yield* client.cancelJob(address);
      yield* client.detachJob(address);
      const output = yield* client.readJobOutput({
        ...address,
        stream: "stdout",
        sequence: 0,
        maximumBytes: 16,
      });

      expect(Object.keys(job).sort()).toEqual([
        "attached",
        "command",
        "detail",
        "exitCode",
        "jobId",
        "sessionId",
        "status",
      ]);
      expect(output.data).toEqual(new Uint8Array());
    }).pipe(Effect.provide(live));
  }),
);

scopedLive("adds the foreground lease to the Job request timeout", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-job-lease-timeout-" });
    const live = layerLive(`${directory}/daemon.sock`, "40 millis", "20 millis");

    const job = yield* Effect.gen(function* () {
      const client = yield* LoomClient;
      return yield* client.startJob({
        sessionId,
        jobId: JobId.make("job-lease-timeout"),
        command: "exit 0",
        attached: true,
        foregroundLeaseMillis: 50,
      });
    }).pipe(Effect.provide(live));

    expect(job.status).toBe("Succeeded");
  }),
);
