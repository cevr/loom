import { BunServices } from "@effect/platform-bun";
import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { CellEvaluation, CellLedgerEntry, CellLedgerState } from "@cvr/loom-protocol";
import { CellLedger, CellLedgerClaim } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer } from "effect";
import { layerLoomSqlite, layerSqliteCellLedger } from "../src/index.js";

const owner = {
  sessionId: SessionId.make("session-1"),
  agentId: AgentId.make("agent-1"),
};
const request = CellLedgerEntry.make({
  ...owner,
  cellId: CellId.make("cell-1"),
  source: "40 + 2",
  state: CellLedgerState.cases.Accepted.make({}),
});
const scopedLive = it.scopedLive.layer(BunServices.layer);

const withLedger = <A, E>(filename: string, effect: Effect.Effect<A, E, CellLedger>) =>
  effect.pipe(
    Effect.provide(layerSqliteCellLedger.pipe(Layer.provide(layerLoomSqlite({ filename })))),
    Effect.scoped,
  );

scopedLive("claims one Cell identity and keeps its terminal outcome", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-cell-ledger-" });
    const filename = `${directory}/loom.sqlite`;

    const first = yield* withLedger(
      filename,
      Effect.gen(function* () {
        const ledger = yield* CellLedger;
        const claim = yield* ledger.claim(request);
        yield* ledger.evaluating(request);
        yield* ledger.complete(
          request,
          CellEvaluation.make({ cellId: request.cellId, display: "42", bindings: [] }),
        );
        return claim;
      }),
    );

    const restored = yield* withLedger(
      filename,
      Effect.gen(function* () {
        const ledger = yield* CellLedger;
        return yield* ledger.claim(request);
      }),
    );

    expect(CellLedgerClaim.$is("Accepted")(first)).toBe(true);
    expect(CellLedgerClaim.$is("Existing")(restored)).toBe(true);
    expect(restored.entry.state).toEqual(
      CellLedgerState.cases.Succeeded.make({
        evaluation: CellEvaluation.make({ cellId: request.cellId, display: "42", bindings: [] }),
      }),
    );
  }),
);

scopedLive("interrupts unfinished Cells during startup reconciliation", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-cell-reconcile-" });
    const filename = `${directory}/loom.sqlite`;

    yield* withLedger(
      filename,
      Effect.gen(function* () {
        const ledger = yield* CellLedger;
        yield* ledger.claim(request);
        yield* ledger.evaluating(request);
      }),
    );

    const claim = yield* withLedger(
      filename,
      Effect.gen(function* () {
        const ledger = yield* CellLedger;
        yield* ledger.reconcile;
        return yield* ledger.claim(request);
      }),
    );

    expect(CellLedgerClaim.$is("Existing")(claim)).toBe(true);
    expect(claim.entry.state).toHaveProperty("_tag", "Interrupted");
    expect(claim.entry.state).toHaveProperty("error.reason", "DaemonRestart");
  }),
);
