import {
  AcceptedWorkflowRun,
  type WorkflowRunAddress,
  WorkflowIncarnationId,
  WorkflowRequestDigest,
  WorkflowRunExecution,
  WorkflowRunId,
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
import { LoomDynamicWorkflow } from "./loom-dynamic-workflow.js";

export interface WorkflowRunAcceptanceShape {
  readonly accept: (
    request: WorkflowRunRequest,
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

const acceptanceError = (operation: WorkflowRunAcceptanceError["operation"]) => (cause: object) =>
  new WorkflowRunAcceptanceError({
    operation,
    message: Inspectable.toStringUnknown(cause),
  });

const makeDigestRequest = (crypto: Crypto.Crypto) =>
  Effect.fn("WorkflowRunAcceptance.digest")(
    function* (request: WorkflowRunRequest) {
      const encoded = yield* encodeRequest(request);
      const digest = yield* canonicalJsonSha256(encoded).pipe(
        Effect.provideService(Crypto.Crypto, crypto),
      );
      return WorkflowRequestDigest.make(`sha256:${digest}`);
    },
    Effect.tapError((cause) => Effect.logError("Workflow request digest failed.", cause)),
    Effect.mapError(acceptanceError("digest")),
  );

const makeWorkflowRunCandidate = (crypto: Crypto.Crypto) =>
  Effect.fn("WorkflowRunAcceptance.mint")(function* (request: WorkflowRunRequest) {
    const incarnationId = yield* crypto.randomUUIDv7.pipe(
      Effect.map(WorkflowIncarnationId.make),
      Effect.tapError((cause) => Effect.logError("Workflow incarnation mint failed.", cause)),
      Effect.mapError(acceptanceError("mint")),
    );
    const workflowRunId = WorkflowRunId.make(
      yield* LoomDynamicWorkflow.executionId(WorkflowRunExecution.make({ incarnationId, request })),
    );
    return { incarnationId, workflowRunId };
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
  const digestRequest = makeDigestRequest(crypto);
  const makeCandidate = makeWorkflowRunCandidate(crypto);

  const accept = Effect.fn("WorkflowRunAcceptance.accept")(function* (
    received: WorkflowRunRequest,
  ) {
    const request = normalizeRequest(received);
    const identity = {
      sessionId: request.sessionId,
      name: request.definition.name,
      version: request.definition.version,
      key: request.key,
    };
    const digest = yield* digestRequest(request);
    const { incarnationId, workflowRunId } = yield* makeCandidate(request);
    const accepted = yield* store.claim(identity, digest, incarnationId, workflowRunId);
    if (accepted.digest !== digest) {
      return yield* new WorkflowIdentityConflictError({
        identity,
        acceptedDigest: accepted.digest,
        receivedDigest: digest,
      });
    }
    return AcceptedWorkflowRun.make({
      incarnationId: accepted.incarnationId,
      workflowRunId: accepted.workflowRunId,
      request,
      digest,
    });
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
