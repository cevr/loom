import { CellId } from "@cvr/loom-domain";
import { maximumCellDisplayLength } from "@cvr/loom-protocol";
import { describe, expect, it } from "effect-bun-test";
import { BunServices } from "@effect/platform-bun";
import { Effect, FileSystem } from "effect";
import { makeInProcessCodeKernel } from "../src/index.js";

describe("persistent Bun Code Kernel", () => {
  it.effect("keeps declarations between cells", () =>
    Effect.gen(function* () {
      const kernel = yield* makeInProcessCodeKernel;
      yield* kernel.evaluate({
        cellId: CellId.make("cell-1"),
        source: "const answer: number = 40",
      });
      const result = yield* kernel.evaluate({
        cellId: CellId.make("cell-2"),
        source: "answer + 2",
      });

      expect(result.display).toBe("42");
      expect(result.bindings).toContain("answer");
    }),
  );

  it.effect("supports top-level await", () =>
    Effect.gen(function* () {
      const kernel = yield* makeInProcessCodeKernel;
      const result = yield* kernel.evaluate({
        cellId: CellId.make("cell-1"),
        source: "await Promise.resolve(42)",
      });

      expect(result.display).toBe("42");
    }),
  );
});

describe("Bun Code Kernel display and declarations", () => {
  it.effect("captures console output with the final Cell value", () =>
    Effect.gen(function* () {
      const kernel = yield* makeInProcessCodeKernel;
      const result = yield* kernel.evaluate({
        cellId: CellId.make("cell-console"),
        source: 'console.log("job", { jobId: "job-1" }); 42',
      });

      expect(result.display).toContain("job { jobId: 'job-1' }");
      expect(result.display).toEndWith("42");
    }),
  );

  it.effect("supports declaration changes", () =>
    Effect.gen(function* () {
      const kernel = yield* makeInProcessCodeKernel;
      yield* kernel.evaluate({
        cellId: CellId.make("cell-1"),
        source: "const value: number = 1",
      });
      const result = yield* kernel.evaluate({
        cellId: CellId.make("cell-2"),
        source: "const value: number = 7; value",
      });

      expect(result.display).toBe("7");
    }),
  );
});

it.scopedLive.layer(BunServices.layer)(
  "writes a large file through the single Cell control surface and records one preview",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-cell-write-" });
      const path = `${directory}/nested/large.ts`;
      const kernel = yield* makeInProcessCodeKernel;
      const result = yield* kernel.evaluate({
        cellId: CellId.make("cell-large-write"),
        source: `await loom.write(${Bun.inspect(path)}, Array.from({ length: 5_000 }, (_, index) => \`export const line_\${index} = \${index};\`).join("\\n"))`,
      });

      expect(result.display).toContain("Wrote");
      expect(result.fileChanges).toHaveLength(1);
      expect(result.fileChanges[0]?.newText.split("\n")).toHaveLength(5_000);
      expect(yield* fs.readFileString(path)).toContain("export const line_4999 = 4999;");
    }),
);

it.effect("limits the Cell display before it crosses the process boundary", () =>
  Effect.gen(function* () {
    const kernel = yield* makeInProcessCodeKernel;
    const result = yield* kernel.evaluate({
      cellId: CellId.make("cell-large-display"),
      source: `"x".repeat(${maximumCellDisplayLength * 2})`,
    });

    expect(result.display.length).toBeLessThan(maximumCellDisplayLength + 32);
    expect(result.display).toEndWith("[output truncated]");
  }),
);

describe("persistent Bun Code Kernel recovery", () => {
  it.effect("continues after a failed cell", () =>
    Effect.gen(function* () {
      const kernel = yield* makeInProcessCodeKernel;
      const failure = yield* kernel
        .evaluate({
          cellId: CellId.make("cell-1"),
          source: "throw new Error('cell failed')",
        })
        .pipe(Effect.flip);
      const result = yield* kernel.evaluate({
        cellId: CellId.make("cell-2"),
        source: "6 * 7",
      });

      expect(failure).toHaveProperty("_tag", "CellExecutionError");
      expect(result.display).toBe("42");
    }),
  );

  it.effect("clears declarations on reset", () =>
    Effect.gen(function* () {
      const kernel = yield* makeInProcessCodeKernel;
      yield* kernel.evaluate({
        cellId: CellId.make("cell-1"),
        source: "const retained = 42",
      });
      yield* kernel.reset;
      const failure = yield* kernel
        .evaluate({
          cellId: CellId.make("cell-2"),
          source: "retained",
        })
        .pipe(Effect.flip);

      expect(failure).toHaveProperty("_tag", "CellExecutionError");
    }),
  );
});
