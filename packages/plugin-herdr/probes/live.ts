import { BunRuntime } from "@effect/platform-bun";
import {
  ActorActivity,
  ActorStateProjection,
  ActorSubject,
  AgentId,
  SessionId,
} from "@cvr/loom-domain";
import { layerHerdrClient, runHerdrPlugin, type HerdrPluginConfig } from "@cvr/loom-plugin-herdr";
import { ActorStateHub, type ActorStateHubShape, makeActorStateHub } from "@cvr/loom-runtime";
import { Config, Effect } from "effect";

const sessionId = SessionId.make("loom-herdr-live-probe");
const agentId = AgentId.make("loom-live-agent");
const subject = ActorSubject.cases.Agent.make({ sessionId, agentId });

const publish = Effect.fn("HerdrLiveProbe.publish")(function* (
  hub: ActorStateHubShape,
  activity: ActorActivity,
  revision: number,
  runId: string,
  label: string,
) {
  yield* hub.publish(ActorStateProjection.make({ subject, activity, revision }));
  yield* Effect.sleep("250 millis");
  yield* Effect.logInfo(`LOOM_HERDR_STATE ${runId} ${label}`);
  yield* Effect.sleep("4750 millis");
});

const program = Effect.scoped(
  Effect.gen(function* () {
    const herdrEnv = yield* Config.string("HERDR_ENV");
    if (herdrEnv !== "1") {
      return yield* Effect.die(new Error("The live probe must run inside Herdr."));
    }

    const config: HerdrPluginConfig = {
      socketPath: yield* Config.string("HERDR_SOCKET_PATH"),
      paneId: yield* Config.string("HERDR_PANE_ID"),
      source: "herdr:loom",
      agent: "loom",
      sessionId,
    };
    const runId = yield* Config.string("LOOM_HERDR_PROBE_RUN").pipe(Config.withDefault("manual"));
    const hub = yield* makeActorStateHub;

    yield* runHerdrPlugin(config).pipe(
      Effect.provideService(ActorStateHub, hub),
      Effect.provide(layerHerdrClient(config)),
    );

    yield* publish(
      hub,
      ActorActivity.cases.Working.make({ message: "running live probe" }),
      1,
      runId,
      "working",
    );
    yield* publish(
      hub,
      ActorActivity.cases.Blocked.make({ message: "live approval probe" }),
      2,
      runId,
      "blocked",
    );
    yield* publish(
      hub,
      ActorActivity.cases.Failed.make({ message: "live failure probe" }),
      3,
      runId,
      "failed",
    );
    yield* publish(hub, ActorActivity.cases.Idle.make({}), 4, runId, "idle");
  }),
);

BunRuntime.runMain(program);
