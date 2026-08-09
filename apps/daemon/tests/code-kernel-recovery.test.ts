import { BunServices } from "@effect/platform-bun";
import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { layerCellJournal, layerCodeKernelFactory } from "@cvr/loom-platform-bun";
import { CellEvaluation } from "@cvr/loom-protocol";
import {
  AgentActor,
  CellJournal,
  CellJournalStoreError,
  CodeKernel,
  CodeKernelFactory,
  layerAgentActor,
} from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer, Ref } from "effect";
import { TestRunner } from "effect/unstable/cluster";

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
    const journalLive = layerCellJournal({ filename });
    const live = Layer.merge(
      layerAgentActor.pipe(
        Layer.provide([
          TestRunner.layer,
          journalLive,
          layerCodeKernelFactory({ entryPath: workerEntry, cellTimeout: "250 millis" }),
        ]),
      ),
      journalLive,
    );

    yield* Effect.gen(function* () {
      yield* AgentActor.EvaluateCell.execute({
        ...owner,
        cellId: CellId.make("cell-state"),
        source: "const retained = 42",
      });
      const blocked = yield* AgentActor.EvaluateCell.execute({
        ...owner,
        cellId: CellId.make("cell-blocked"),
        source: "await new Promise(() => {})",
      }).pipe(Effect.flip);
      const missingState = yield* AgentActor.EvaluateCell.execute({
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

it.scopedLive("does not evaluate a Cell when the journal write fails", () =>
  Effect.gen(function* () {
    const evaluated = yield* Ref.make(false);
    const cellId = CellId.make("cell-unwritten");
    const kernel = CodeKernel.of({
      evaluate: () =>
        Ref.set(evaluated, true).pipe(
          Effect.as(CellEvaluation.make({ cellId, display: "42", bindings: [] })),
        ),
      reset: Effect.void,
      close: Effect.void,
    });
    const factory = CodeKernelFactory.of({ spawn: () => Effect.succeed(kernel) });
    const journal = CellJournal.of({
      append: () =>
        Effect.fail(new CellJournalStoreError({ operation: "append", cause: "disk full" })),
      list: () => Effect.succeed([]),
    });

    const live = layerAgentActor.pipe(
      Layer.provide([
        TestRunner.layer,
        Layer.succeed(CodeKernelFactory, factory),
        Layer.succeed(CellJournal, journal),
      ]),
    );
    const failure = yield* AgentActor.EvaluateCell.execute({
      ...owner,
      cellId,
      source: "21 * 2",
    }).pipe(Effect.provide(live), Effect.flip);

    expect(failure).toHaveProperty("reason", "JournalFailure");
    expect(yield* Ref.get(evaluated)).toBe(false);
  }),
);

it.scopedLive("keeps Code Kernel bindings inside one Agent owner", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-agent-isolation-" });
    const filename = `${directory}/loom.sqlite`;
    const otherOwner = { ...owner, agentId: AgentId.make("agent-2") };
    const live = layerAgentActor.pipe(
      Layer.provide([
        TestRunner.layer,
        layerCellJournal({ filename }),
        layerCodeKernelFactory({ entryPath: workerEntry }),
      ]),
    );

    yield* Effect.gen(function* () {
      yield* AgentActor.EvaluateCell.execute({
        ...owner,
        cellId: CellId.make("cell-owner-write"),
        source: "const privateBinding = 42",
      });
      const sameOwner = yield* AgentActor.EvaluateCell.execute({
        ...owner,
        cellId: CellId.make("cell-owner-read"),
        source: "privateBinding",
      });
      const otherResult = yield* AgentActor.EvaluateCell.execute({
        ...otherOwner,
        cellId: CellId.make("cell-other-read"),
        source: "privateBinding",
      }).pipe(Effect.flip);

      expect(sameOwner.display).toBe("42");
      expect(otherResult).toHaveProperty("_tag", "CellExecutionError");
    }).pipe(Effect.provide(live));
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("resets only the selected Agent Code Kernel", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-agent-reset-" });
    const otherOwner = { ...owner, agentId: AgentId.make("agent-2") };
    const live = layerAgentActor.pipe(
      Layer.provide([
        TestRunner.layer,
        layerCellJournal({ filename: `${directory}/loom.sqlite` }),
        layerCodeKernelFactory({ entryPath: workerEntry }),
      ]),
    );

    yield* Effect.gen(function* () {
      yield* AgentActor.EvaluateCell.execute({
        ...owner,
        cellId: CellId.make("cell-owner-write"),
        source: "const privateBinding = 42",
      });
      yield* AgentActor.EvaluateCell.execute({
        ...otherOwner,
        cellId: CellId.make("cell-other-write"),
        source: "const otherBinding = 7",
      });
      yield* AgentActor.ResetCodeKernel.execute(owner);
      const resetOwner = yield* AgentActor.EvaluateCell.execute({
        ...owner,
        cellId: CellId.make("cell-owner-after-reset"),
        source: "privateBinding",
      }).pipe(Effect.flip);
      const otherAfterReset = yield* AgentActor.EvaluateCell.execute({
        ...otherOwner,
        cellId: CellId.make("cell-other-after-reset"),
        source: "otherBinding",
      });

      expect(resetOwner).toHaveProperty("_tag", "CellExecutionError");
      expect(otherAfterReset.display).toBe("7");
    }).pipe(Effect.provide(live));
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("starts a fresh Code Kernel after an Agent closes it", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-agent-close-" });
    const live = layerAgentActor.pipe(
      Layer.provide([
        TestRunner.layer,
        layerCellJournal({ filename: `${directory}/loom.sqlite` }),
        layerCodeKernelFactory({ entryPath: workerEntry }),
      ]),
    );

    yield* Effect.gen(function* () {
      yield* AgentActor.EvaluateCell.execute({
        ...owner,
        cellId: CellId.make("cell-before-close"),
        source: "const closedBinding = 9",
      });
      yield* AgentActor.CloseCodeKernel.execute(owner);
      const afterClose = yield* AgentActor.EvaluateCell.execute({
        ...owner,
        cellId: CellId.make("cell-after-close"),
        source: "closedBinding",
      }).pipe(Effect.flip);

      expect(afterClose).toHaveProperty("_tag", "CellExecutionError");
    }).pipe(Effect.provide(live));
  }).pipe(Effect.provide(BunServices.layer)),
);
