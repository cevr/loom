import {
  ActorActivity,
  ActorStateProjection,
  ActorSubject,
  AgentId,
  JobId,
  SessionId,
} from "@cvr/loom-domain";
import { ActorStateHub, type ActorStateHubShape, makeActorStateHub } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, Option, Queue } from "effect";
import {
  HerdrClient,
  runHerdrPlugin,
  type HerdrClientShape,
  type HerdrState,
} from "../src/index.js";

type HerdrCall =
  | {
      readonly _tag: "Report";
      readonly state: HerdrState;
      readonly message: Option.Option<string>;
    }
  | { readonly _tag: "Release" };

const releaseCall = { _tag: "Release" } satisfies HerdrCall;

interface TestState {
  readonly calls: Queue.Queue<HerdrCall>;
  readonly hub: ActorStateHubShape;
  readonly sessionId: SessionId;
  readonly agent: ActorSubject;
  readonly job: ActorSubject;
}

const publish = (
  state: TestState,
  subject: ActorSubject,
  activity: ActorActivity,
  revision: number,
) => state.hub.publish(ActorStateProjection.make({ subject, activity, revision }));

const verifyProjections = Effect.fn("HerdrPluginTest.verifyProjections")(function* (
  state: TestState,
) {
  expect(yield* Queue.take(state.calls)).toMatchObject({ state: "idle" });

  yield* publish(state, state.agent, ActorActivity.cases.Working.make({}), 1);
  expect(yield* Queue.take(state.calls)).toMatchObject({ state: "working" });

  yield* publish(
    state,
    state.job,
    ActorActivity.cases.Blocked.make({ message: "approval required" }),
    1,
  );
  expect(yield* Queue.take(state.calls)).toMatchObject({
    state: "blocked",
    message: Option.some("approval required"),
  });

  yield* publish(state, state.job, ActorActivity.cases.Stopped.make({}), 2);
  expect(yield* Queue.take(state.calls)).toMatchObject({ state: "working" });

  yield* publish(state, state.agent, ActorActivity.cases.Idle.make({}), 2);
  expect(yield* Queue.take(state.calls)).toMatchObject({ state: "idle" });
});

it.effect("publishes the session actor state and releases the pane", () =>
  Effect.gen(function* () {
    const sessionId = SessionId.make("session-1");
    const calls = yield* Queue.unbounded<HerdrCall>();
    const hub = yield* makeActorStateHub;
    const testState: TestState = {
      calls,
      hub,
      sessionId,
      agent: ActorSubject.cases.Agent.make({
        sessionId,
        agentId: AgentId.make("agent-1"),
      }),
      job: ActorSubject.cases.Job.make({
        sessionId,
        jobId: JobId.make("job-1"),
      }),
    };
    const client = HerdrClient.of({
      report: (state, message) => Queue.offer(calls, { _tag: "Report", state, message }),
      release: Queue.offer(calls, releaseCall),
    } satisfies HerdrClientShape);

    yield* Effect.scoped(
      Effect.gen(function* () {
        yield* runHerdrPlugin({
          socketPath: "/tmp/unused.sock",
          paneId: "w1:p1",
          source: "herdr:loom",
          agent: "loom",
          sessionId,
        }).pipe(
          Effect.provideService(ActorStateHub, hub),
          Effect.provideService(HerdrClient, client),
        );

        yield* verifyProjections(testState);
      }),
    );

    expect(yield* Queue.take(calls)).toEqual(releaseCall);
  }),
);
