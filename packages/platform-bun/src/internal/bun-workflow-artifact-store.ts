import { workflowArtifactId, type WorkspaceRoot } from "@cvr/loom-domain";
import {
  WorkflowArtifactNotFoundError,
  WorkflowArtifactReference,
  WorkflowArtifactStore,
  WorkflowArtifactStoreError,
  type WorkflowArtifactStoreShape,
} from "@cvr/loom-runtime";
import { Crypto, Effect, FileSystem, Layer, Option, Schema } from "effect";

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Json));
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json));

export interface BunWorkflowArtifactStoreConfig {
  readonly workspaceRoot: WorkspaceRoot;
}

export const makeBunWorkflowArtifactStore = Effect.fn("BunWorkflowArtifactStore.make")(function* (
  config: BunWorkflowArtifactStoreConfig,
) {
  const fs = yield* FileSystem.FileSystem;
  const crypto = yield* Crypto.Crypto;
  const directory = `${config.workspaceRoot}/.loom/artifacts`;
  const pathFor = (artifactId: WorkflowArtifactReference["artifactId"]) =>
    `${directory}/${encodeURIComponent(artifactId)}.json`;
  const storeError = (artifactId: WorkflowArtifactReference["artifactId"], cause: object) =>
    new WorkflowArtifactStoreError({ artifactId, cause });

  return WorkflowArtifactStore.of({
    store: (write, context) => {
      const artifactId = workflowArtifactId(context.activityKey);
      const target = pathFor(artifactId);
      return Effect.gen(function* () {
        yield* fs.makeDirectory(directory, { recursive: true });
        const temporary = `${target}.${yield* crypto.randomUUIDv4}.tmp`;
        yield* fs
          .writeFileString(temporary, yield* encodeJson(write.value))
          .pipe(
            Effect.andThen(fs.rename(temporary, target)),
            Effect.ensuring(fs.remove(temporary).pipe(Effect.ignore)),
          );
        return WorkflowArtifactReference.make({ artifactId });
      }).pipe(Effect.mapError((cause) => storeError(artifactId, cause)));
    },
    read: (reference) =>
      Effect.gen(function* () {
        const encoded = yield* fs.readFileString(pathFor(reference.artifactId)).pipe(
          Effect.map(Option.some),
          Effect.catchReason("PlatformError", "NotFound", () => Effect.succeed(Option.none())),
          Effect.mapError((cause) => storeError(reference.artifactId, cause)),
        );
        if (Option.isNone(encoded)) {
          return yield* new WorkflowArtifactNotFoundError({ artifactId: reference.artifactId });
        }
        return yield* decodeJson(encoded.value).pipe(
          Effect.mapError((cause) => storeError(reference.artifactId, cause)),
        );
      }),
  } satisfies WorkflowArtifactStoreShape);
});

export const layerBunWorkflowArtifactStore = (config: BunWorkflowArtifactStoreConfig) =>
  Layer.effect(WorkflowArtifactStore, makeBunWorkflowArtifactStore(config));
