import { BunServices } from "@effect/platform-bun";
import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { CodeKernel, layerCellJournal, layerCodeKernel } from "@cvr/loom-platform-bun";
import { CellEvaluation } from "@cvr/loom-protocol";
import { CellJournal, CellJournalStoreError } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer, Ref } from "effect";
import { evaluateJournaledCell } from "../src/rpc-handlers.js";

const workerEntry = new URL("../../code-kernel/src/main.ts", import.meta.url).pathname;
const owner = {
  sessionId: SessionId.make("session-1"),
  agentId: AgentId.make("agent-1"),
};

it.scopedLive("preserves the Cell journal when a blocked Code Kernel is replaced", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-recovery-" });
    const filename = `${directory}/loom.sqlite`;
    const live = Layer.merge(
      layerCellJournal({ filename }),
      layerCodeKernel({ entryPath: workerEntry, cellTimeout: "100 millis" }),
    );

    yield* Effect.gen(function* () {
      yield* evaluateJournaledCell({
        ...owner,
        cellId: CellId.make("cell-state"),
        source: "const retained = 42",
      });
      const blocked = yield* evaluateJournaledCell({
        ...owner,
        cellId: CellId.make("cell-blocked"),
        source: "await new Promise(() => {})",
      }).pipe(Effect.flip);
      const missingState = yield* evaluateJournaledCell({
        ...owner,
        cellId: CellId.make("cell-after-replacement"),
        source: "retained",
      }).pipe(Effect.flip);
      const journal = yield* CellJournal;
      const entries = yield* journal.list(owner);

      expect(blocked).toHaveProperty("reason", "TimedOut");
      expect(missingState).toHaveProperty("_tag", "CellExecutionError");
      expect(entries.map((entry) => entry.source)).toEqual([
        "const retained = 42",
        "await new Promise(() => {})",
        "retained",
      ]);
    }).pipe(Effect.provide(live));
  }).pipe(Effect.provide(BunServices.layer)),
);

it.effect("does not evaluate a Cell when the journal write fails", () =>
  Effect.gen(function* () {
    const evaluated = yield* Ref.make(false);
    const cellId = CellId.make("cell-unwritten");
    const kernel = CodeKernel.of({
      evaluate: () =>
        Ref.set(evaluated, true).pipe(
          Effect.as(CellEvaluation.make({ cellId, display: "42", bindings: [] })),
        ),
      reset: Effect.void,
    });
    const journal = CellJournal.of({
      append: () =>
        Effect.fail(new CellJournalStoreError({ operation: "append", cause: "disk full" })),
      list: () => Effect.succeed([]),
    });

    const failure = yield* evaluateJournaledCell({
      ...owner,
      cellId,
      source: "21 * 2",
    }).pipe(
      Effect.provideService(CodeKernel, kernel),
      Effect.provideService(CellJournal, journal),
      Effect.flip,
    );

    expect(failure).toHaveProperty("reason", "JournalFailure");
    expect(yield* Ref.get(evaluated)).toBe(false);
  }),
);
