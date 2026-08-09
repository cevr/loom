import { BunServices } from "@effect/platform-bun";
import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { CodeKernelFactory } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, Fiber, FileSystem } from "effect";
import { layerCodeKernelFactory, makeCodeKernel } from "../src/index.js";

const workerEntry = new URL("../../../apps/code-kernel/src/main.ts", import.meta.url).pathname;
const stderrExitEntry = new URL("./fixtures/stderr-exit.ts", import.meta.url).pathname;
const resetExitEntry = new URL("./fixtures/reset-exit.ts", import.meta.url).pathname;
const owner = {
  sessionId: SessionId.make("session-1"),
  agentId: AgentId.make("agent-1"),
};

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

it.scopedLive("replaces an interrupted Code Kernel before the next Cell", () =>
  Effect.gen(function* () {
    const kernel = yield* makeCodeKernel({ entryPath: workerEntry });
    const running = yield* Effect.forkChild(
      kernel.evaluate({
        cellId: CellId.make("cell-interrupted"),
        source: "await new Promise(() => {})",
      }),
    );
    yield* Effect.sleep("50 millis");
    yield* Fiber.interrupt(running);
    const next = yield* kernel.evaluate({
      cellId: CellId.make("cell-after-interrupt"),
      source: "6 * 7",
    });

    expect(next.display).toBe("42");
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

it.scopedLive("returns bounded stderr and a retained diagnostic file when startup fails", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-diagnostic-" });
    yield* Effect.gen(function* () {
      const factory = yield* CodeKernelFactory;
      const kernel = yield* factory.spawn(owner);
      const failure = yield* kernel
        .evaluate({ cellId: CellId.make("cell-startup-failure"), source: "42" })
        .pipe(Effect.flip);
      expect(failure).toHaveProperty("reason", "ProcessExited");
      expect(failure).toHaveProperty("diagnostic.exitCode", 23);
      expect(failure).toHaveProperty("diagnostic.stderrTail", "loom kernel boot failure\n");
      const files = yield* fs.readDirectory(`${directory}/session-1/agent-1`);
      const contents = yield* Effect.forEach(files, (file) =>
        fs.readFileString(`${directory}/session-1/agent-1/${file}`),
      );
      expect(contents).toEqual(["loom kernel boot failure\n"]);
    }).pipe(
      Effect.provide(
        layerCodeKernelFactory({ entryPath: stderrExitEntry, diagnosticsDirectory: directory }),
      ),
    );
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("returns a typed failure when the Code Kernel executable cannot start", () =>
  Effect.gen(function* () {
    const kernel = yield* makeCodeKernel({
      entryPath: workerEntry,
      executable: "/loom/missing/bun",
    });
    const failure = yield* kernel
      .evaluate({ cellId: CellId.make("cell-spawn-failure"), source: "42" })
      .pipe(Effect.flip);

    expect(failure).toHaveProperty("_tag", "CellInterruptedError");
    expect(failure).toHaveProperty("reason", "ProcessExited");
    expect(failure).toHaveProperty("diagnostic", undefined);
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("retains only the configured number of stderr files", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-retention-" });
    yield* Effect.gen(function* () {
      const factory = yield* CodeKernelFactory;
      const kernel = yield* factory.spawn(owner);
      yield* Effect.forEach([1, 2, 3], (id) =>
        kernel
          .evaluate({ cellId: CellId.make(`cell-retention-${id}`), source: "42" })
          .pipe(Effect.flip),
      );
    }).pipe(
      Effect.provide(
        layerCodeKernelFactory({
          entryPath: stderrExitEntry,
          diagnosticsDirectory: directory,
          maxFilesPerOwner: 2,
          crashLoopLimit: 10,
        }),
      ),
    );

    const files = yield* fs.readDirectory(`${directory}/session-1/agent-1`);
    expect(files).toHaveLength(2);
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("blocks a repeated Code Kernel crash loop until its cooldown ends", () =>
  Effect.gen(function* () {
    const kernel = yield* makeCodeKernel({
      entryPath: workerEntry,
      crashLoopLimit: 2,
      crashLoopCooldown: "100 millis",
    });
    const crash = (cellId: string) =>
      kernel
        .evaluate({
          cellId: CellId.make(cellId),
          source: 'const processModule = await import("node:process"); processModule.exit(17)',
        })
        .pipe(Effect.flip);

    yield* crash("cell-crash-1");
    yield* crash("cell-crash-2");
    const blocked = yield* crash("cell-crash-blocked");
    yield* Effect.sleep("120 millis");
    const recovered = yield* kernel.evaluate({
      cellId: CellId.make("cell-after-cooldown"),
      source: "6 * 7",
    });

    expect(blocked).toHaveProperty("reason", "CrashLoop");
    expect(recovered.display).toBe("42");
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("forgets process failures outside the crash-loop window", () =>
  Effect.gen(function* () {
    const kernel = yield* makeCodeKernel({
      entryPath: workerEntry,
      crashLoopLimit: 2,
      crashLoopWindow: "20 millis",
      crashLoopCooldown: "1 second",
    });
    const crash = (cellId: string) =>
      kernel
        .evaluate({
          cellId: CellId.make(cellId),
          source: 'const processModule = await import("node:process"); processModule.exit(17)',
        })
        .pipe(Effect.flip);

    yield* crash("cell-window-1");
    yield* Effect.sleep("30 millis");
    yield* crash("cell-window-2");
    const recovered = yield* kernel.evaluate({
      cellId: CellId.make("cell-window-recovered"),
      source: "6 * 7",
    });

    expect(recovered.display).toBe("42");
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("replaces a Code Kernel that exits before reset", () =>
  Effect.gen(function* () {
    const kernel = yield* makeCodeKernel({ entryPath: resetExitEntry });
    yield* kernel.evaluate({
      cellId: CellId.make("cell-exit-before-reset"),
      source: "42",
    });
    yield* kernel.reset;
    const recovered = yield* kernel.evaluate({
      cellId: CellId.make("cell-after-reset"),
      source: "42",
    });

    expect(recovered.display).toBe("42");
  }).pipe(Effect.provide(BunServices.layer)),
);
