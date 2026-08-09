import { AgentId, CellId, SessionId } from "@cvr/loom-domain";
import { BunServices } from "@effect/platform-bun";
import { CodeKernelFactory } from "@cvr/loom-runtime";
import { expect, it } from "effect-bun-test";
import { Effect, FileSystem, Layer, Option } from "effect";
import { layerCodeKernelFactory } from "../src/index.js";

const workerEntry = new URL("../../../apps/code-kernel/src/main.ts", import.meta.url).pathname;
const stderrExitEntry = new URL("./fixtures/stderr-exit.ts", import.meta.url).pathname;
const largeStderrExitEntry = new URL("./fixtures/large-stderr-exit.ts", import.meta.url).pathname;
const owner = {
  sessionId: SessionId.make("session-1"),
  agentId: AgentId.make("agent-1"),
};

it.scopedLive("bounds each diagnostic file and retains the stderr tail", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-byte-limit-" });
    yield* Effect.gen(function* () {
      const factory = yield* CodeKernelFactory;
      const kernel = yield* factory.spawn(owner);
      yield* kernel
        .evaluate({ cellId: CellId.make("cell-large-stderr"), source: "42" })
        .pipe(Effect.flip);
    }).pipe(
      Effect.provide(
        layerCodeKernelFactory({
          entryPath: largeStderrExitEntry,
          diagnosticsDirectory: directory,
          maxFileBytes: 128,
          stderrTailCharacters: 32,
        }),
      ),
    );
    const ownerDirectory = `${directory}/session-1/agent-1`;
    const files = yield* fs.readDirectory(ownerDirectory);
    expect(files).toHaveLength(1);
    const file = yield* Effect.fromOption(Option.fromUndefinedOr(files[0]));
    const contents = yield* fs.readFile(`${ownerDirectory}/${file}`);
    expect(contents).toHaveLength(128);
    const text = new TextDecoder().decode(contents);
    expect(text).toContain("head:");
    expect(text).toContain("[loom: stderr truncated]");
    expect(text).toContain(":tail\n");
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("prunes the oldest diagnostic across owners and removes its empty directory", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-global-limit-" });
    const otherOwner = { ...owner, agentId: AgentId.make("agent-2") };
    yield* Effect.gen(function* () {
      const factory = yield* CodeKernelFactory;
      const first = yield* factory.spawn(owner);
      const second = yield* factory.spawn(otherOwner);
      yield* first.evaluate({ cellId: CellId.make("cell-oldest"), source: "42" }).pipe(Effect.flip);
      yield* Effect.sleep("2 millis");
      yield* second
        .evaluate({ cellId: CellId.make("cell-newer-1"), source: "42" })
        .pipe(Effect.flip);
      yield* Effect.sleep("2 millis");
      yield* second
        .evaluate({ cellId: CellId.make("cell-newer-2"), source: "42" })
        .pipe(Effect.flip);
    }).pipe(
      Effect.provide(
        layerCodeKernelFactory({
          entryPath: stderrExitEntry,
          diagnosticsDirectory: directory,
          maxFilesPerOwner: 5,
          maxFilesTotal: 2,
          crashLoopLimit: 10,
        }),
      ),
    );
    expect(yield* fs.exists(`${directory}/session-1/agent-1`)).toBe(false);
    const files = (yield* fs.readDirectory(directory, { recursive: true })).filter((entry) =>
      entry.endsWith(".stderr.log"),
    );
    expect(files).toHaveLength(2);
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("cleans stale and oversized diagnostics when the factory starts", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-restart-cleanup-" });
    const ownerDirectory = `${directory}/session-1/agent-1`;
    yield* fs.makeDirectory(ownerDirectory, { recursive: true });
    yield* fs.writeFileString(`${ownerDirectory}/1-1.stderr.log`, "old");
    yield* fs.writeFileString(`${ownerDirectory}/2-2.stderr.log`, "new");
    yield* fs.writeFileString(`${ownerDirectory}/3-3.stderr.log`, "oversized");
    yield* fs.makeDirectory(`${directory}/empty-session/empty-agent`, { recursive: true });
    yield* CodeKernelFactory.pipe(
      Effect.asVoid,
      Effect.provide(
        layerCodeKernelFactory({
          entryPath: workerEntry,
          diagnosticsDirectory: directory,
          maxFileBytes: 4,
          maxFilesPerOwner: 1,
          maxFilesTotal: 1,
        }),
      ),
    );
    expect(yield* fs.readDirectory(ownerDirectory)).toEqual(["2-2.stderr.log"]);
    expect(yield* fs.exists(`${directory}/empty-session`)).toBe(false);
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("recreates an owner directory after self-eviction", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-self-prune-" });
    yield* Effect.gen(function* () {
      const factory = yield* CodeKernelFactory;
      const kernel = yield* factory.spawn(owner);
      yield* kernel
        .evaluate({ cellId: CellId.make("cell-self-prune-1"), source: "42" })
        .pipe(Effect.flip);
      const second = yield* kernel
        .evaluate({ cellId: CellId.make("cell-self-prune-2"), source: "42" })
        .pipe(Effect.flip);
      expect(second).toHaveProperty("diagnostic.stderrPath", expect.any(String));
    }).pipe(
      Effect.provide(
        layerCodeKernelFactory({
          entryPath: stderrExitEntry,
          diagnosticsDirectory: directory,
          maxFilesPerOwner: 1,
          crashLoopLimit: 10,
        }),
      ),
    );
    expect(yield* fs.readDirectory(`${directory}/session-1/agent-1`)).toHaveLength(1);
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("does not follow an owner directory symlink outside the diagnostic root", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-containment-" });
    const outside = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-outside-" });
    yield* fs.writeFileString(`${outside}/1-1.stderr.log`, "keep");
    yield* fs.symlink(outside, `${directory}/session-evil`);
    yield* fs.makeDirectory(`${directory}/session-1`);
    yield* fs.symlink(outside, `${directory}/session-1/agent-evil`);
    const evilOwner = {
      sessionId: SessionId.make("session-evil"),
      agentId: AgentId.make("agent-1"),
    };
    const evilAgent = {
      sessionId: SessionId.make("session-1"),
      agentId: AgentId.make("agent-evil"),
    };
    yield* Effect.gen(function* () {
      const factory = yield* CodeKernelFactory;
      const kernel = yield* factory.spawn(evilOwner);
      const failure = yield* kernel
        .evaluate({ cellId: CellId.make("cell-contained"), source: "42" })
        .pipe(Effect.flip);
      expect(failure).toHaveProperty("diagnostic.stderrPath", undefined);
      const agentKernel = yield* factory.spawn(evilAgent);
      const agentFailure = yield* agentKernel
        .evaluate({ cellId: CellId.make("cell-agent-contained"), source: "42" })
        .pipe(Effect.flip);
      expect(agentFailure).toHaveProperty("diagnostic.stderrPath", undefined);
    }).pipe(
      Effect.provide(
        layerCodeKernelFactory({ entryPath: stderrExitEntry, diagnosticsDirectory: directory }),
      ),
    );
    expect(yield* fs.readFileString(`${outside}/1-1.stderr.log`)).toBe("keep");
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("runs without a diagnostic file when active kernels fill the global limit", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-active-limit-" });
    const otherOwner = { ...owner, agentId: AgentId.make("agent-2") };
    yield* Effect.gen(function* () {
      const factory = yield* CodeKernelFactory;
      const first = yield* factory.spawn(owner);
      const second = yield* factory.spawn(otherOwner);
      yield* first.evaluate({ cellId: CellId.make("cell-active"), source: "42" });
      const failure = yield* second
        .evaluate({
          cellId: CellId.make("cell-without-file"),
          source: 'const processModule = await import("node:process"); processModule.exit(17)',
        })
        .pipe(Effect.flip);
      expect(failure).toHaveProperty("diagnostic.stderrPath", undefined);
    }).pipe(
      Effect.provide(
        layerCodeKernelFactory({
          entryPath: workerEntry,
          diagnosticsDirectory: directory,
          maxFilesTotal: 1,
        }),
      ),
    );
    expect(yield* fs.exists(`${directory}/session-1/agent-2`)).toBe(false);
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("skips a broken owner entry during allocation cleanup", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "loom-kernel-broken-link-" });
    yield* fs.symlink(`${directory}/missing`, `${directory}/broken-session`);
    yield* Effect.gen(function* () {
      const factory = yield* CodeKernelFactory;
      const kernel = yield* factory.spawn(owner);
      const failure = yield* kernel
        .evaluate({ cellId: CellId.make("cell-after-broken-link"), source: "42" })
        .pipe(Effect.flip);
      expect(failure).toHaveProperty("diagnostic.stderrPath", expect.any(String));
    }).pipe(
      Effect.provide(
        layerCodeKernelFactory({ entryPath: stderrExitEntry, diagnosticsDirectory: directory }),
      ),
    );
  }).pipe(Effect.provide(BunServices.layer)),
);

it.scopedLive("allows the in-memory stderr tail to be disabled", () =>
  Effect.gen(function* () {
    const factory = yield* CodeKernelFactory;
    const kernel = yield* factory.spawn(owner);
    const failure = yield* kernel
      .evaluate({ cellId: CellId.make("cell-without-tail"), source: "42" })
      .pipe(Effect.flip);
    expect(failure).toHaveProperty("diagnostic.stderrTail", undefined);
  }).pipe(
    Effect.provide(
      layerCodeKernelFactory({ entryPath: stderrExitEntry, stderrTailCharacters: 0 }).pipe(
        Layer.provide(BunServices.layer),
      ),
    ),
  ),
);
