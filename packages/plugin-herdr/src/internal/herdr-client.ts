import { BunSocket } from "@effect/platform-bun";
import type { SessionId } from "@cvr/loom-domain";
import { Clock, Context, Deferred, Effect, Fiber, Layer, Ref, Schema, Semaphore } from "effect";
import type { Socket } from "effect/unstable/socket/Socket";
import { HerdrClientError } from "./herdr-client-error.js";

export { HerdrClientError } from "./herdr-client-error.js";

export type HerdrState = "idle" | "working" | "blocked" | "unknown";

export interface HerdrPluginConfig {
  readonly socketPath: string;
  readonly paneId: string;
  readonly source: string;
  readonly agent: string;
  readonly sessionId: SessionId;
}

export interface HerdrClientShape {
  readonly report: (
    state: HerdrState,
    message: string | undefined,
  ) => Effect.Effect<void, HerdrClientError>;
  readonly release: Effect.Effect<void, HerdrClientError>;
}

export class HerdrClient extends Context.Service<HerdrClient, HerdrClientShape>()(
  "@cvr/loom-plugin-herdr/HerdrClient",
) {}

const ReportAgentRequest = Schema.Struct({
  id: Schema.String,
  method: Schema.Literal("pane.report_agent"),
  params: Schema.Struct({
    pane_id: Schema.String,
    source: Schema.String,
    agent: Schema.String,
    state: Schema.Literals(["idle", "working", "blocked", "unknown"]),
    message: Schema.NullOr(Schema.String),
    seq: Schema.Natural,
    agent_session_id: Schema.String,
  }),
});

const ReleaseAgentRequest = Schema.Struct({
  id: Schema.String,
  method: Schema.Literal("pane.release_agent"),
  params: Schema.Struct({
    pane_id: Schema.String,
    source: Schema.String,
    agent: Schema.String,
    seq: Schema.Natural,
  }),
});

const HerdrRequest = Schema.Union([ReportAgentRequest, ReleaseAgentRequest]);
const HerdrRequestJson = Schema.fromJsonString(HerdrRequest);

const HerdrResponse = Schema.Union([
  Schema.Struct({
    id: Schema.String,
    result: Schema.Struct({ type: Schema.Literal("ok") }),
  }),
  Schema.Struct({
    id: Schema.String,
    error: Schema.Struct({
      code: Schema.String,
      message: Schema.String,
    }),
  }),
]);
const HerdrResponseJson = Schema.fromJsonString(HerdrResponse);

const encodeRequest = Schema.encodeEffect(HerdrRequestJson);
const decodeResponse = Schema.decodeUnknownEffect(HerdrResponseJson);

type NextSequence = () => Effect.Effect<number>;

const exchange = Effect.fn("HerdrClient.exchange")(function* (socket: Socket, encoded: string) {
  const response = yield* Deferred.make<string>();
  const buffer = yield* Ref.make("");
  const reader = yield* socket
    .runString((chunk) =>
      Effect.gen(function* () {
        yield* Ref.update(buffer, (current) => current + chunk);
        const current = yield* Ref.get(buffer);
        const newline = current.indexOf("\n");
        if (newline >= 0) {
          yield* Deferred.succeed(response, current.slice(0, newline));
        }
      }),
    )
    .pipe(Effect.forkChild);

  const write = yield* socket.writer;
  yield* write(`${encoded}\n`);
  const raw = yield* Deferred.await(response).pipe(Effect.timeout("750 millis"));
  yield* Fiber.interrupt(reader);
  return raw;
});

const sendRequest = Effect.fn("HerdrClient.sendRequest")(function* (
  socketPath: string,
  operation: string,
  request: typeof HerdrRequest.Type,
) {
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const encoded = yield* encodeRequest(request);
      const socket = yield* BunSocket.makeNet({
        path: socketPath,
        openTimeout: "500 millis",
      });
      const raw = yield* exchange(socket, encoded);

      const decoded = yield* decodeResponse(raw);
      if (decoded.id !== request.id) {
        return yield* Effect.fail({
          code: "response_id_mismatch",
          message: `Expected ${request.id}, received ${decoded.id}`,
        });
      }
      if ("error" in decoded) {
        return yield* Effect.fail(decoded.error);
      }
    }),
  ).pipe(
    Effect.mapError(
      (cause) =>
        new HerdrClientError({
          operation,
          cause,
        }),
    ),
  );
});

const makeNextSequence: Effect.Effect<NextSequence> = Effect.gen(function* () {
  const initialTime = yield* Clock.currentTimeMillis;
  const sequence = yield* Ref.make(initialTime * 1000);
  return Effect.fn("HerdrClient.nextSequence")(function* () {
    const now = yield* Clock.currentTimeMillis;
    return yield* Ref.modify(sequence, (current) => {
      const next = Math.max(current + 1, now * 1000);
      return [next, next];
    });
  });
});

const makeReport = (config: HerdrPluginConfig, nextSequence: NextSequence) =>
  Effect.fn("HerdrClient.report")(function* (state: HerdrState, message: string | undefined) {
    const seq = yield* nextSequence();
    const request = ReportAgentRequest.make({
      id: `${config.source}:${seq}`,
      method: "pane.report_agent",
      params: {
        pane_id: config.paneId,
        source: config.source,
        agent: config.agent,
        state,
        message: message ?? null,
        seq,
        agent_session_id: config.sessionId,
      },
    });
    yield* sendRequest(config.socketPath, "report", request);
  });

const makeRelease = (config: HerdrPluginConfig, nextSequence: NextSequence) =>
  Effect.gen(function* () {
    const seq = yield* nextSequence();
    const request = ReleaseAgentRequest.make({
      id: `${config.source}:release:${seq}`,
      method: "pane.release_agent",
      params: {
        pane_id: config.paneId,
        source: config.source,
        agent: config.agent,
        seq,
      },
    });
    yield* sendRequest(config.socketPath, "release", request);
  });

const makeHerdrClient = (config: HerdrPluginConfig): Effect.Effect<HerdrClientShape> =>
  Effect.gen(function* () {
    const nextSequence = yield* makeNextSequence;
    const semaphore = yield* Semaphore.make(1);
    const report = makeReport(config, nextSequence);
    const release = makeRelease(config, nextSequence);

    return HerdrClient.of({
      report: (state, message) => Semaphore.withPermit(semaphore, report(state, message)),
      release: Semaphore.withPermit(semaphore, release),
    });
  });

export const layerHerdrClient = (config: HerdrPluginConfig): Layer.Layer<HerdrClient> =>
  Layer.effect(HerdrClient, makeHerdrClient(config));
