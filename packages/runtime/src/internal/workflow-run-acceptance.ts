import { AcceptedWorkflowRun, WorkflowRequestDigest, WorkflowRunRequest } from "@cvr/loom-domain";
import { WorkflowIdentityConflictError, WorkflowRunAcceptanceError } from "@cvr/loom-protocol";
import { Context, Crypto, Effect, Inspectable, Layer, Schema } from "effect";
import { canonicalJsonSha256 } from "effect-encore";
import { WorkflowRunAcceptanceStore } from "./workflow-run-acceptance-store.js";
import { workflowIdentityFromRequest } from "./workflow-identity.js";

export interface WorkflowRunAcceptanceShape {
  readonly accept: (
    request: WorkflowRunRequest,
  ) => Effect.Effect<
    AcceptedWorkflowRun,
    WorkflowIdentityConflictError | WorkflowRunAcceptanceError
  >;
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
  ) {
    const request = normalizeRequest(received);
    const identity = workflowIdentityFromRequest(request);
    const digest = yield* digestRequest(request);
    const acceptedDigest = yield* store.claim(identity, digest);
    if (acceptedDigest !== digest) {
      return yield* new WorkflowIdentityConflictError({
        identity,
        acceptedDigest,
        receivedDigest: digest,
      });
    }
    return AcceptedWorkflowRun.make({ identity, request, digest });
  });

  return WorkflowRunAcceptance.of({ accept });
});

export const layerWorkflowRunAcceptance: Layer.Layer<
  WorkflowRunAcceptance,
  never,
  Crypto.Crypto | WorkflowRunAcceptanceStore
> = Layer.effect(WorkflowRunAcceptance, makeWorkflowRunAcceptance);
