import { ActorActivity, type ActorStateProjection, type SessionId } from "@cvr/loom-domain";
import { ActorStateHub, type ActorStateSnapshot } from "@cvr/loom-runtime";
import { Effect, Layer, type Scope, Stream } from "effect";
import { HerdrClient, type HerdrPluginConfig, type HerdrState } from "./herdr-client.js";

interface HerdrProjection {
  readonly state: HerdrState;
  readonly message: string | undefined;
}

type ActivityProjection = HerdrProjection | "Working" | "Inactive";

const isSessionProjection = (sessionId: SessionId, projection: ActorStateProjection): boolean =>
  projection.subject.sessionId === sessionId;

const projectActivity = (projection: ActorStateProjection): ActivityProjection =>
  ActorActivity.match<ActivityProjection>(projection.activity, {
    Blocked: (activity) => ({
      state: "blocked",
      message: activity.message,
    }),
    Failed: (activity) => ({
      state: "blocked",
      message: activity.message,
    }),
    Working: () => "Working",
    Idle: () => "Inactive",
    Stopped: () => "Inactive",
  });

const toHerdrProjection = (snapshot: ActorStateSnapshot, sessionId: SessionId): HerdrProjection => {
  let working = false;
  for (const projection of snapshot.values()) {
    if (!isSessionProjection(sessionId, projection)) {
      continue;
    }
    const activity = projectActivity(projection);
    if (activity === "Working") {
      working = true;
      continue;
    }
    if (activity !== "Inactive") {
      return activity;
    }
  }
  if (working) {
    return { state: "working", message: undefined };
  }
  return { state: "idle", message: undefined };
};

const sameProjection = (left: HerdrProjection, right: HerdrProjection): boolean =>
  left.state === right.state && left.message === right.message;

const logClientFailure = (operation: string) =>
  Effect.fn(`HerdrPlugin.${operation}Failure`)(function* (cause: unknown) {
    yield* Effect.logWarning(`Herdr ${operation} failed`, cause);
  });

export const runHerdrPlugin = (
  config: HerdrPluginConfig,
): Effect.Effect<void, never, ActorStateHub | HerdrClient | Scope.Scope> =>
  Effect.gen(function* () {
    const states = yield* ActorStateHub;
    const client = yield* HerdrClient;

    yield* Effect.addFinalizer(() =>
      client.release.pipe(Effect.catch(logClientFailure("release"))),
    );

    yield* states.snapshots.pipe(
      Stream.map((snapshot) => toHerdrProjection(snapshot, config.sessionId)),
      Stream.changesWith(sameProjection),
      Stream.buffer({ capacity: 1, strategy: "sliding" }),
      Stream.runForEach((projection) =>
        client
          .report(projection.state, projection.message)
          .pipe(Effect.catch(logClientFailure("report"))),
      ),
      Effect.forkScoped,
    );
  });

export const layerHerdrPlugin = (
  config: HerdrPluginConfig,
): Layer.Layer<never, never, ActorStateHub | HerdrClient> =>
  Layer.effectDiscard(runHerdrPlugin(config));
