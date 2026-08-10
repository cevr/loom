import {
  ActorActivity,
  ActorStateProjection,
  ActorSubject,
  type WorkflowRunAddress,
} from "@cvr/loom-domain";
import { WorkflowRunState, type WorkflowRunError } from "@cvr/loom-protocol";
import {
  Context,
  type Duration,
  Effect,
  FiberMap,
  Inspectable,
  Layer,
  Match,
  Ref,
  Schema,
  Stream,
} from "effect";
import { WorkflowEngine } from "effect/unstable/workflow";
import { isTerminal, type PeekResult } from "effect-encore";
import { ActorStateHub, type ActorStateHubShape } from "./actor-state-hub.js";
import { LoomDynamicWorkflow } from "./loom-dynamic-workflow.js";
import { WorkflowRunRetention } from "./workflow-run-retention.js";
import type { WorkflowRunRetentionError } from "./workflow-run-retention-error.js";

export interface WorkflowRunStatePublisherShape {
  readonly watch: (address: WorkflowRunAddress) => Effect.Effect<void>;
  readonly retire: (address: WorkflowRunAddress) => Effect.Effect<void>;
}

export class WorkflowRunStatePublisher extends Context.Service<
  WorkflowRunStatePublisher,
  WorkflowRunStatePublisherShape
>()("@cvr/loom-runtime/WorkflowRunStatePublisher") {}

export interface WorkflowRunStatePublisherOptions {
  readonly stateLease: Duration.Input;
}

export const toWorkflowRunState = (
  state: PeekResult<Schema.Json, WorkflowRunError>,
): WorkflowRunState =>
  Match.value(state).pipe(
    Match.tag("Defect", ({ cause }) =>
      WorkflowRunState.cases.Defect.make({ message: Inspectable.toStringUnknown(cause) }),
    ),
    Match.orElse((result) => result),
  );

const toActorActivity = (state: WorkflowRunState): ActorActivity =>
  WorkflowRunState.match<ActorActivity>(state, {
    Pending: () => ActorActivity.cases.Working.make({}),
    Success: () => ActorActivity.cases.Stopped.make({}),
    Failure: ({ error }) =>
      ActorActivity.cases.Failed.make({ message: Inspectable.toStringUnknown(error) }),
    Interrupted: () => ActorActivity.cases.Stopped.make({}),
    Defect: ({ message }) => ActorActivity.cases.Failed.make({ message }),
    Suspended: () => ActorActivity.cases.Blocked.make({ message: "Workflow run is suspended." }),
  });

type PublishActivity = (
  address: WorkflowRunAddress,
  activity: ActorActivity,
) => Effect.Effect<void>;

const logRetentionFailure = (address: WorkflowRunAddress) =>
  Effect.catch((error: WorkflowRunRetentionError) =>
    Effect.logError("Workflow Run retention failed.", error).pipe(
      Effect.annotateLogs({
        sessionId: address.sessionId,
        workflowRunId: address.workflowRunId,
      }),
    ),
  );

const makePublishActivity = (
  hub: ActorStateHubShape,
  revisions: Ref.Ref<ReadonlyMap<string, number>>,
): PublishActivity =>
  Effect.fn("WorkflowRunStatePublisher.publish")(function* (
    address: WorkflowRunAddress,
    activity: ActorActivity,
  ) {
    const revision = yield* Ref.modify(revisions, (current) => {
      const nextRevision = (current.get(address.workflowRunId) ?? 0) + 1;
      const next = new Map(current);
      if (ActorActivity.guards.Stopped(activity)) {
        next.delete(address.workflowRunId);
      } else {
        next.set(address.workflowRunId, nextRevision);
      }
      return [nextRevision, next];
    });
    yield* hub.publish(
      ActorStateProjection.make({
        subject: ActorSubject.cases.WorkflowRun.make(address),
        activity,
        revision,
      }),
    );
  });

const makeCompleteActivity = (
  options: WorkflowRunStatePublisherOptions,
  publish: PublishActivity,
  retention: WorkflowRunRetention["Service"],
) =>
  Effect.fn("WorkflowRunStatePublisher.completeActivity")(function* (
    address: WorkflowRunAddress,
    activity: ActorActivity,
  ) {
    if (!ActorActivity.guards.Failed(activity) && !ActorActivity.guards.Stopped(activity)) return;
    yield* retention
      .retireAfterLease(address, options.stateLease)
      .pipe(logRetentionFailure(address));
    if (ActorActivity.guards.Failed(activity)) {
      yield* publish(address, ActorActivity.cases.Stopped.make({}));
    }
  });

const makeStartupOperations = (
  options: WorkflowRunStatePublisherOptions,
  engine: WorkflowEngine.WorkflowEngine["Service"],
  retention: WorkflowRunRetention["Service"],
) => {
  const inspect = (address: WorkflowRunAddress) =>
    LoomDynamicWorkflow.peekAt(address.workflowRunId).pipe(
      Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
    );
  const retire = Effect.fn("WorkflowRunStatePublisher.retire")(function* (
    address: WorkflowRunAddress,
  ) {
    const state = yield* inspect(address);
    if (isTerminal(state)) {
      yield* retention
        .retireExpired(address, options.stateLease)
        .pipe(logRetentionFailure(address));
    }
  });
  return { retire };
};

const makeWorkflowRunStatePublisher = (options: WorkflowRunStatePublisherOptions) =>
  Effect.gen(function* () {
    const hub = yield* ActorStateHub;
    const engine = yield* WorkflowEngine.WorkflowEngine;
    const retention = yield* WorkflowRunRetention;
    const watchers = yield* FiberMap.make<string>();
    const revisions = yield* Ref.make<ReadonlyMap<string, number>>(new Map());
    const publishActivity = makePublishActivity(hub, revisions);
    const completeActivity = makeCompleteActivity(options, publishActivity, retention);

    const watch = Effect.fn("WorkflowRunStatePublisher.watch")(function* (
      address: WorkflowRunAddress,
    ) {
      yield* LoomDynamicWorkflow.watchAt(address.workflowRunId).pipe(
        Stream.map(toWorkflowRunState),
        Stream.map(toActorActivity),
        Stream.runForEach((activity) =>
          publishActivity(address, activity).pipe(
            Effect.andThen(completeActivity(address, activity)),
          ),
        ),
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        FiberMap.run(watchers, address.workflowRunId, { onlyIfMissing: true }),
      );
    });

    return WorkflowRunStatePublisher.of({
      ...makeStartupOperations(options, engine, retention),
      watch,
    });
  });

export const layerWorkflowRunStatePublisher = (
  options: WorkflowRunStatePublisherOptions = { stateLease: "5 minutes" },
) => Layer.effect(WorkflowRunStatePublisher, makeWorkflowRunStatePublisher(options));
