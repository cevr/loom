/* oxlint-disable effect/noGlobals -- This test server is the host boundary for the real Bun Unix socket transport. */
import { BunServices } from "@effect/platform-bun";
import { SessionId } from "@cvr/loom-domain";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Schema } from "effect";
import { HerdrClient, layerHerdrClient, type HerdrPluginConfig } from "../src/index.js";

const RequestEnvelope = Schema.Struct({
  id: Schema.String,
  method: Schema.Literals(["pane.report_agent", "pane.release_agent"]),
  params: Schema.Struct({ seq: Schema.Natural }),
});
const decodeRequest = Schema.decodeUnknownSync(Schema.fromJsonString(RequestEnvelope));

const OkResponse = Schema.Struct({
  id: Schema.String,
  result: Schema.Struct({ type: Schema.Literal("ok") }),
});
const encodeResponse = Schema.encodeSync(Schema.fromJsonString(OkResponse));

it.scoped("sends ordered reports through the Herdr Unix socket", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({
      prefix: "loom-herdr-client-test-",
    });
    const socketPath = `${directory}/herdr.sock`;
    const requests: Array<typeof RequestEnvelope.Type> = [];
    const server = yield* Effect.acquireRelease(
      Effect.sync(() =>
        Bun.listen({
          unix: socketPath,
          socket: {
            data(socket, data) {
              const request = decodeRequest(data.toString().trim());
              requests.push(request);
              socket.write(`${encodeResponse({ id: request.id, result: { type: "ok" } })}\n`);
              socket.end();
            },
          },
        }),
      ),
      (listener) => Effect.sync(() => listener.stop(true)),
    );
    expect(server.unix).toBe(socketPath);

    const config: HerdrPluginConfig = {
      socketPath,
      paneId: "w1:p1",
      source: "herdr:loom",
      agent: "loom",
      sessionId: SessionId.make("session-1"),
    };

    yield* Effect.gen(function* () {
      const client = yield* HerdrClient;
      yield* client.report("working", "running a job");
      yield* client.report("idle", undefined);
      yield* client.release;
    }).pipe(Effect.provide(layerHerdrClient(config)));

    expect(requests.map((request) => request.method)).toEqual([
      "pane.report_agent",
      "pane.report_agent",
      "pane.release_agent",
    ]);
    expect(requests[1]?.params.seq).toBeGreaterThan(requests[0]?.params.seq ?? 0);
    expect(requests[2]?.params.seq).toBeGreaterThan(requests[1]?.params.seq ?? 0);
  }).pipe(Effect.provide(BunServices.layer)),
);
