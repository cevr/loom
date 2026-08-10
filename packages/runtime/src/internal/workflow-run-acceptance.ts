import {
  AcceptedWorkflowRun,
  type WorkflowRunAddress,
  WorkflowRequestDigest,
  type WorkflowRunId,
  WorkflowRunRequest,
} from "@cvr/loom-domain";
import {
  WorkflowIdentityConflictError,
  WorkflowRunAcceptanceError,
  WorkflowRunNotFoundError,
} from "@cvr/loom-protocol";
import { Context, Crypto, Effect, Inspectable, Layer, Option, Schema } from "effect";
import { canonicalJsonSha256 } from "effect-encore";
import { WorkflowRunAcceptanceStore } from "./workflow-run-acceptance-store.js";
import { workflowIdentityFromRequest } from "./workflow-identity.js";

export interface WorkflowRunAcceptanceShape {
  readonly accept: (
    request: WorkflowRunRequest,
    workflowRunId: WorkflowRunId,
  ) => Effect.Effect<
    AcceptedWorkflowRun,
    WorkflowIdentityConflictError | WorkflowRunAcceptanceError
  >;
  readonly authorize: (
    address: WorkflowRunAddress,
  ) => Effect.Effect<void, WorkflowRunNotFoundError | WorkflowRunAcceptanceError>;
  readonly list: WorkflowRunAcceptanceStore["Service"]["list"];
}

export class WorkflowRunAcceptance extends Context.Service<
  WorkflowRunAcceptance,
  WorkflowRunAcceptanceShape
>()("@cvr/loom-runtime/WorkflowRunAcceptance") {}

const encodeRequest = Schema.encodeEffect(WorkflowRunRequest);

const normalizeNames = <A extends string>(names: ReadonlyArray<A>): ReadonlyArray<A> =>
  Array.from(new Set(names)).sort();

const normalizeRequest = (request: WorkflowRunRequest): WorkflowRunRequest =>
  WorkflowRunRequest.make({
    ...request,
    definition: {
      ...request.definition,
      capabilities: normalizeNames(request.definition.capabilities),
      signals: normalizeNames(request.definition.signals),
    },
  });

const makeAuthorizeWorkflowRun = (store: WorkflowRunAcceptanceStore["Service"]) =>
  Effect.fn("WorkflowRunAcceptance.authorize")(function* (address: WorkflowRunAddress) {
    const identity = yield* store.lookup(address.workflowRunId);
    if (Option.exists(identity, (accepted) => accepted.sessionId === address.sessionId)) return;
    return yield* new WorkflowRunNotFoundError({ address });
  });

export const makeWorkflowRunAcceptance: Effect.Effect<
  WorkflowRunAcceptanceShape,
  never,
  Crypto.Crypto | WorkflowRunAcceptanceStore
> = Effect.gen(function* () {
  const crypto = yield* Crypto.Crypto;
  const store = yield* WorkflowRunAcceptanceStore;

  const digestRequest = Effect.fn("WorkflowRunAcceptance.digest")(
    function* (request: WorkflowRunRequest) {
      const encoded = yield* encodeRequest(request);
      const digest = yield* canonicalJsonSha256(encoded).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
      );
      return WorkflowRequestDigest.make(`sha256:${digest}`);
    },
    Effect.tapError((cause) => Effect.logError("Workflow request digest failed.", cause)),
    Effect.mapError(
      (cause) =>
        new WorkflowRunAcceptanceError({
          operation: "digest",
          message: Inspectable.toStringUnknown(cause),
        }),
    ),
  );

  const accept = Effect.fn("WorkflowRunAcceptance.accept")(function* (
    received: WorkflowRunRequest,
    workflowRunId: WorkflowRunId,
  ) {
    const request = normalizeRequest(received);
    const identity = workflowIdentityFromRequest(request);
    const digest = yield* digestRequest(request);
    const acceptedDigest = yield* store.claim(identity, digest, workflowRunId);
    if (acceptedDigest !== digest) {
      return yield* new WorkflowIdentityConflictError({
        identity,
        acceptedDigest,
        receivedDigest: digest,
      });
    }
    return AcceptedWorkflowRun.make({ workflowRunId, identity, request, digest });
  });

  return WorkflowRunAcceptance.of({
    accept,
    authorize: makeAuthorizeWorkflowRun(store),
    list: store.list,
  });
});

export const layerWorkflowRunAcceptance: Layer.Layer<
  WorkflowRunAcceptance,
  never,
  Crypto.Crypto | WorkflowRunAcceptanceStore
> = Layer.effect(WorkflowRunAcceptance, makeWorkflowRunAcceptance);
