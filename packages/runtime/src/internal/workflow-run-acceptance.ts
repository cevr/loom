import {
  AcceptedWorkflowRun,
  WorkflowIdentity,
  WorkflowRequestDigest,
  WorkflowRunRequest,
} from "@cvr/loom-domain";
import { WorkflowIdentityConflictError } from "@cvr/loom-protocol";
import { Context, Crypto, Effect, Layer, Schema } from "effect";
import { canonicalJsonSha256 } from "effect-encore";
import { WorkflowRunAcceptanceError } from "./workflow-run-acceptance-error.js";
import { WorkflowRunAcceptanceStore } from "./workflow-run-acceptance-store.js";

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

const identityOf = (request: WorkflowRunRequest): WorkflowIdentity =>
  WorkflowIdentity.make({
    sessionId: request.sessionId,
    name: request.definition.name,
    version: request.definition.version,
    key: request.key,
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
    Effect.mapError((cause) => new WorkflowRunAcceptanceError({ operation: "digest", cause })),
  );

  const accept = Effect.fn("WorkflowRunAcceptance.accept")(function* (
    received: WorkflowRunRequest,
  ) {
    const request = normalizeRequest(received);
    const identity = identityOf(request);
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
