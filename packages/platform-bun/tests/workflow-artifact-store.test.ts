import { BunServices } from "@effect/platform-bun";
import {
  ArtifactId,
  SessionId,
  WorkflowActivityKey,
  WorkflowRunId,
  WorkflowStepId,
  WorkspaceRoot,
  workflowArtifactId,
} from "@cvr/loom-domain";
import {
  WorkflowActivityContext,
  WorkflowArtifactNotFoundError,
  WorkflowArtifactReference,
  WorkflowArtifactStoreError,
  WorkflowArtifactWrite,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Ref } from "effect";
import { makeBunWorkflowArtifactStore } from "../src/index.js";

const context = WorkflowActivityContext.make({
  activityKey: WorkflowActivityKey.make("workflow/artifact"),
  sessionId: SessionId.make("session-1"),
  workflowRunId: WorkflowRunId.make("workflow-1"),
});

const write = WorkflowArtifactWrite.make({
  stepId: WorkflowStepId.make("artifact-step"),
  value: { result: "complete" },
});

it.scopedLive.layer(BunServices.layer)("publishes one complete Artifact after a failed write", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-artifact-atomic-" });
    const workspaceRoot = WorkspaceRoot.make(directory);
    const failNextWrite = yield* Ref.make(true);
    const faultingFs = FileSystem.FileSystem.of({
      ...fs,
      writeFileString: (path, value, encoding) =>
        Ref.getAndSet(failNextWrite, false).pipe(
          Effect.flatMap((fail) => {
            if (!fail) return fs.writeFileString(path, value, encoding);
            return fs
              .writeFileString(path, value.slice(0, 1), encoding)
              .pipe(Effect.andThen(fs.readFileString(`${path}.missing`)), Effect.asVoid);
          }),
        ),
    });
    const artifacts = yield* makeBunWorkflowArtifactStore({ workspaceRoot }).pipe(
      Effect.provideService(FileSystem.FileSystem, faultingFs),
    );
    const artifactId = workflowArtifactId(context.activityKey);
    const target = `${directory}/.loom/artifacts/${encodeURIComponent(artifactId)}.json`;

    const failure = yield* artifacts.store(write, context).pipe(Effect.flip);
    expect(failure).toBeInstanceOf(WorkflowArtifactStoreError);
    expect(yield* fs.exists(target)).toBe(false);
    expect(yield* fs.readDirectory(`${directory}/.loom/artifacts`)).toEqual([]);

    const reference = yield* artifacts.store(write, context);
    expect(reference.artifactId).toBe(artifactId);
    expect(yield* fs.readFileString(target)).toBe('{"result":"complete"}');
    expect(yield* artifacts.read(reference)).toEqual({ result: "complete" });
  }),
);

it.scopedLive.layer(BunServices.layer)("returns a typed error for a missing Artifact", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-artifact-missing-" });
    const artifacts = yield* makeBunWorkflowArtifactStore({
      workspaceRoot: WorkspaceRoot.make(directory),
    });
    const reference = WorkflowArtifactReference.make({
      artifactId: ArtifactId.make("missing-artifact"),
    });

    const failure = yield* artifacts.read(reference).pipe(Effect.flip);
    expect(failure).toBeInstanceOf(WorkflowArtifactNotFoundError);
    expect(failure.artifactId).toBe(reference.artifactId);
  }),
);
