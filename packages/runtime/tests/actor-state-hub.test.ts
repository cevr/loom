import {
  ActorActivity,
  ActorStateProjection,
  ActorSubject,
  AgentId,
  SessionId,
} from "@cvr/loom-domain";
import { expect, it } from "effect-bun-test";
import { Effect } from "effect";
import { makeActorStateHub } from "../src/index.js";

const sessionId = SessionId.make("session-1");
const agentId = AgentId.make("agent-1");
const subject = ActorSubject.cases.Agent.make({ sessionId, agentId });

it.effect("keeps only the latest live actor projection", () =>
  Effect.gen(function* () {
    const hub = yield* makeActorStateHub;
    yield* hub.publish(
      ActorStateProjection.make({
        subject,
        activity: ActorActivity.cases.Working.make({ message: "running" }),
        revision: 2,
      }),
    );
    yield* hub.publish(
      ActorStateProjection.make({
        subject,
        activity: ActorActivity.cases.Idle.make({}),
        revision: 1,
      }),
    );

    const snapshot = yield* hub.snapshot;
    expect(snapshot.size).toBe(1);
    expect(snapshot.values().next().value?.activity).toHaveProperty("_tag", "Working");

    yield* hub.publish(
      ActorStateProjection.make({
        subject,
        activity: ActorActivity.cases.Stopped.make({}),
        revision: 3,
      }),
    );

    expect((yield* hub.snapshot).size).toBe(0);
  }),
);
