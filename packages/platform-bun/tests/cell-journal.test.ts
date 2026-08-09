import { BunServices } from "@effect/platform-bun";
import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { CellJournal } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem } from "effect";
import { layerCellJournal } from "../src/index.js";

const owner = {
  sessionId: SessionId.make("session-1"),
  agentId: AgentId.make("agent-1"),
};

const withJournal = <A, E>(
  filename: string,
  effect: Effect.Effect<A, E, CellJournal>,
): Effect.Effect<A, E | unknown> =>
  effect.pipe(Effect.provide(layerCellJournal({ filename })), Effect.scoped);

it.scopedLive("keeps the ordered Cell journal across SQLite client restarts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-cell-journal-" });
    const filename = `${directory}/loom.sqlite`;

    yield* withJournal(
      filename,
      Effect.gen(function* () {
        const journal = yield* CellJournal;
        yield* journal.append({
          ...owner,
          cellId: CellId.make("cell-1"),
          source: "const answer = 40",
        });
        yield* journal.append({
          ...owner,
          cellId: CellId.make("cell-2"),
          source: "answer + 2",
        });
      }),
    );

    const entries = yield* withJournal(
      filename,
      Effect.gen(function* () {
        const journal = yield* CellJournal;
        return yield* journal.list(owner);
      }),
    );

    expect(entries[0]?.cellId).toBe(CellId.make("cell-1"));
    expect(entries[1]?.cellId).toBe(CellId.make("cell-2"));
    expect(entries.map((entry) => entry.source)).toEqual(["const answer = 40", "answer + 2"]);
  }).pipe(Effect.provide(BunServices.layer)),
);
