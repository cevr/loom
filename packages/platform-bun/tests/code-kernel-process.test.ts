import { BunServices } from "@effect/platform-bun";
import { CellId } from "@cvr/loom-domain";
import { expect, it } from "effect-bun-test";
import { Effect } from "effect";
import { makeCodeKernel } from "../src/index.js";

const workerEntry = new URL("../../../apps/code-kernel/src/main.ts", import.meta.url).pathname;

it.scopedLive("evaluates persistent TypeScript and imports in a separate Bun process", () =>
  Effect.gen(function* () {
    const kernel = yield* makeCodeKernel({ entryPath: workerEntry });
    yield* kernel.evaluate({
      cellId: CellId.make("cell-1"),
      source: "const answer: number = 40",
    });
    const retained = yield* kernel.evaluate({
      cellId: CellId.make("cell-2"),
      source: "answer + 2",
    });
    const imported = yield* kernel.evaluate({
      cellId: CellId.make("cell-3"),
      source: 'const path = await import("node:path"); path.basename("/loom/kernel")',
    });

    expect(retained.display).toBe("42");
    expect(imported.display).toBe('"kernel"');
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("returns a typed malformed Cell failure without replacing the process", () =>
  Effect.gen(function* () {
    const kernel = yield* makeCodeKernel({ entryPath: workerEntry });
    const failure = yield* kernel
      .evaluate({
        cellId: CellId.make("cell-bad"),
        source: "const broken: =",
      })
      .pipe(Effect.flip);
    const next = yield* kernel.evaluate({
      cellId: CellId.make("cell-next"),
      source: "21 * 2",
    });

    expect(failure).toHaveProperty("_tag", "CellCompilationError");
    expect(next.display).toBe("42");
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("replaces a blocked Code Kernel without replaying mutable state", () =>
  Effect.gen(function* () {
    const kernel = yield* makeCodeKernel({
      entryPath: workerEntry,
      cellTimeout: "100 millis",
    });
    yield* kernel.evaluate({
      cellId: CellId.make("cell-state"),
      source: "const mutableState = 42",
    });
    const failure = yield* kernel
      .evaluate({
        cellId: CellId.make("cell-blocked"),
        source: "await new Promise(() => {})",
      })
      .pipe(Effect.flip);
    const missingState = yield* kernel
      .evaluate({
        cellId: CellId.make("cell-after-block"),
        source: "mutableState",
      })
      .pipe(Effect.flip);

    expect(failure).toHaveProperty("_tag", "CellInterruptedError");
    expect(failure).toHaveProperty("reason", "TimedOut");
    expect(missingState).toHaveProperty("_tag", "CellExecutionError");
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("keeps the daemon process alive after the Code Kernel exits", () =>
  Effect.gen(function* () {
    const kernel = yield* makeCodeKernel({ entryPath: workerEntry });
    const failure = yield* kernel
      .evaluate({
        cellId: CellId.make("cell-exit"),
        source: 'const processModule = await import("node:process"); processModule.exit(17)',
      })
      .pipe(Effect.flip);
    const next = yield* kernel.evaluate({
      cellId: CellId.make("cell-replacement"),
      source: "6 * 7",
    });

    expect(failure).toHaveProperty("_tag", "CellInterruptedError");
    expect(failure).toHaveProperty("reason", "ProcessExited");
    expect(next.display).toBe("42");
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("clears persistent state when the Code Kernel resets", () =>
  Effect.gen(function* () {
    const kernel = yield* makeCodeKernel({ entryPath: workerEntry });
    yield* kernel.evaluate({
      cellId: CellId.make("cell-state"),
      source: "const retained = 42",
    });
    yield* kernel.reset;
    const missingState = yield* kernel
      .evaluate({
        cellId: CellId.make("cell-after-reset"),
        source: "retained",
      })
      .pipe(Effect.flip);

    expect(missingState).toHaveProperty("_tag", "CellExecutionError");
  }).pipe(Effect.provide(BunServices.layer)),
);
