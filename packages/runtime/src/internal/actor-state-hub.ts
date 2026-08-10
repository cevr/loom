import { ActorActivity, ActorSubject, type ActorStateProjection } from "@cvr/loom-domain";
import { Context, Effect, Layer, Option, Stream, SubscriptionRef } from "effect";

export type ActorStateSnapshot = ReadonlyMap<string, ActorStateProjection>;

export interface ActorStateHubShape {
  readonly publish: (projection: ActorStateProjection) => Effect.Effect<void>;
  readonly snapshot: Effect.Effect<ActorStateSnapshot>;
  readonly snapshots: Stream.Stream<ActorStateSnapshot>;
}

export class ActorStateHub extends Context.Service<ActorStateHub, ActorStateHubShape>()(
  "@cvr/loom-runtime/ActorStateHub",
) {}

const subjectKey = (subject: ActorSubject): string =>
  ActorSubject.match(subject, {
    Agent: (agent) => `agent:${agent.sessionId}:${agent.agentId}`,
    Job: (job) => `job:${job.sessionId}:${job.jobId}`,
    WorkflowRun: (run) => `workflow-run:${run.sessionId}:${run.workflowRunId}`,
  });

export const makeActorStateHub: Effect.Effect<ActorStateHubShape> = Effect.gen(function* () {
  const state = yield* SubscriptionRef.make<ActorStateSnapshot>(new Map());

  const publish = Effect.fn("ActorStateHub.publish")(function* (projection: ActorStateProjection) {
    const key = subjectKey(projection.subject);
    yield* SubscriptionRef.update(state, (current) => {
      const previous = Option.fromNullishOr(current.get(key));
      if (Option.exists(previous, (value) => value.revision >= projection.revision)) return current;

      const next = new Map(current);
      if (ActorActivity.guards.Stopped(projection.activity)) {
        next.delete(key);
      } else {
        next.set(key, projection);
      }
      return next;
    });
  });

  return ActorStateHub.of({
    publish,
    snapshot: SubscriptionRef.get(state),
    snapshots: SubscriptionRef.changes(state),
  });
});

export const layerActorStateHub: Layer.Layer<ActorStateHub> = Layer.effect(
  ActorStateHub,
  makeActorStateHub,
);
