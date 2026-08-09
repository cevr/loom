import type { AgentOwner } from "@cvr/loom-domain";
import { Clock, Effect, FileSystem, Option, Path, Scope, Semaphore } from "effect";
import type { Path as PathService } from "effect/Path";
import type { PlatformError } from "effect/PlatformError";

const diagnosticName = /^(\d+)-(?:\d+)\.stderr\.log$/u;

export interface CodeKernelDiagnosticStoreConfig {
  readonly diagnosticsDirectory?: string;
  readonly maxFileBytes?: number;
  readonly maxFilesPerOwner?: number;
  readonly maxFilesTotal?: number;
}

export interface KernelDiagnosticFile {
  readonly path: string;
  readonly maxFileBytes: number;
}

interface StoredFile {
  readonly path: string;
  readonly ownerDirectory: string;
  readonly sessionDirectory: string;
  readonly timestamp: number;
  readonly size: bigint;
}

interface StoreState {
  readonly fs: FileSystem.FileSystem;
  readonly path: PathService;
  readonly directory: string;
  readonly active: Set<string>;
  readonly maxFileBytes: number;
  readonly maxFilesPerOwner: number;
  readonly maxFilesTotal: number;
}

interface CodeKernelDiagnosticStore {
  readonly reserve: (
    owner: AgentOwner,
    pid: number,
  ) => Effect.Effect<KernelDiagnosticFile | undefined, PlatformError, Scope.Scope>;
}

const encodeSegment = (value: string) => encodeURIComponent(value).replaceAll(".", "%2E");
const naturalLimit = (value: number | undefined, fallback: number) =>
  Math.max(0, Math.floor(value ?? fallback));

const isWithin = (path: PathService, root: string, candidate: string) => {
  const relative = path.relative(root, candidate);
  return relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative);
};

const canonicalDirectory = Effect.fn("CodeKernelDiagnosticStore.canonicalDirectory")(function* (
  state: StoreState,
  parent: string,
  name: string,
) {
  const candidate = state.path.join(parent, name);
  yield* state.fs.makeDirectory(candidate, { recursive: true });
  const canonical = yield* state.fs.realPath(candidate);
  if (isWithin(state.path, parent, canonical)) return canonical;
  return undefined;
});

const ensureOwnerDirectory = Effect.fn("CodeKernelDiagnosticStore.ensureOwnerDirectory")(function* (
  state: StoreState,
  root: string,
  owner: AgentOwner,
) {
  const session = yield* canonicalDirectory(state, root, encodeSegment(owner.sessionId));
  if (session === undefined) return undefined;
  const directory = yield* canonicalDirectory(state, session, encodeSegment(owner.agentId));
  if (directory === undefined) return undefined;
  return { directory, session };
});

const readFiles = Effect.fn("CodeKernelDiagnosticStore.readFiles")(function* (
  state: StoreState,
  root: string,
) {
  const files: Array<StoredFile> = [];
  const owners = new Map<string, string>();
  for (const sessionName of yield* state.fs.readDirectory(root)) {
    const sessionPath = state.path.join(root, sessionName);
    const resolvedSession = yield* Effect.option(state.fs.realPath(sessionPath));
    if (Option.isNone(resolvedSession)) continue;
    const canonicalSession = resolvedSession.value;
    if (!isWithin(state.path, root, canonicalSession)) continue;
    const sessionInfo = yield* Effect.option(state.fs.stat(canonicalSession));
    if (Option.isNone(sessionInfo) || sessionInfo.value.type !== "Directory") continue;
    const ownerNames = yield* Effect.option(state.fs.readDirectory(canonicalSession));
    if (Option.isNone(ownerNames)) continue;
    for (const ownerName of ownerNames.value) {
      const ownerPath = state.path.join(canonicalSession, ownerName);
      const resolvedOwner = yield* Effect.option(state.fs.realPath(ownerPath));
      if (Option.isNone(resolvedOwner)) continue;
      const canonicalOwner = resolvedOwner.value;
      if (!isWithin(state.path, root, canonicalOwner)) continue;
      const ownerInfo = yield* Effect.option(state.fs.stat(canonicalOwner));
      if (Option.isNone(ownerInfo) || ownerInfo.value.type !== "Directory") continue;
      owners.set(canonicalOwner, canonicalSession);
      const names = yield* Effect.option(state.fs.readDirectory(canonicalOwner));
      if (Option.isNone(names)) continue;
      for (const name of names.value) {
        const match = diagnosticName.exec(name);
        if (match === null) continue;
        const filePath = state.path.join(canonicalOwner, name);
        const info = yield* Effect.option(state.fs.stat(filePath));
        if (Option.isNone(info) || info.value.type !== "File") continue;
        files.push({
          path: filePath,
          ownerDirectory: canonicalOwner,
          sessionDirectory: canonicalSession,
          timestamp: Number(match[1]),
          size: info.value.size,
        });
      }
    }
  }
  return { files, owners };
});

const removeFiles = (state: StoreState, files: ReadonlyArray<StoredFile>) =>
  Effect.forEach(files, (file) => state.fs.remove(file.path, { force: true }), { discard: true });

const removeEmptyDirectories = Effect.fn("CodeKernelDiagnosticStore.removeEmptyDirectories")(
  function* (state: StoreState, owners: ReadonlyMap<string, string>) {
    for (const [owner, session] of owners) {
      const ownerEntries = yield* Effect.option(state.fs.readDirectory(owner));
      if (Option.isSome(ownerEntries) && ownerEntries.value.length === 0) {
        yield* state.fs.remove(owner, { recursive: true }).pipe(Effect.ignore);
      }
      const sessionEntries = yield* Effect.option(state.fs.readDirectory(session));
      if (Option.isSome(sessionEntries) && sessionEntries.value.length === 0) {
        yield* state.fs.remove(session, { recursive: true }).pipe(Effect.ignore);
      }
    }
  },
);

const cleanup = Effect.fn("CodeKernelDiagnosticStore.cleanup")(function* (state: StoreState) {
  yield* state.fs.makeDirectory(state.directory, { recursive: true });
  const root = yield* state.fs.realPath(state.directory);
  const scan = yield* readFiles(state, root);
  const discovered = scan.files;
  const removed = new Set<string>();
  const oversized = discovered.filter(
    (file) => file.size > BigInt(state.maxFileBytes) && !state.active.has(file.path),
  );
  yield* removeFiles(state, oversized);
  for (const file of oversized) removed.add(file.path);
  const retained = discovered.filter((file) => !removed.has(file.path));
  for (const files of Map.groupBy(retained, (file) => file.ownerDirectory).values()) {
    const stale = files
      .toSorted((left, right) => left.timestamp - right.timestamp)
      .filter((file) => !state.active.has(file.path))
      .slice(0, Math.max(0, files.length - state.maxFilesPerOwner));
    yield* removeFiles(state, stale);
    for (const file of stale) removed.add(file.path);
  }
  const withinOwnerLimit = discovered.filter((file) => !removed.has(file.path));
  const globalStale = withinOwnerLimit
    .toSorted((left, right) => left.timestamp - right.timestamp)
    .filter((file) => !state.active.has(file.path))
    .slice(0, Math.max(0, withinOwnerLimit.length - state.maxFilesTotal));
  yield* removeFiles(state, globalStale);
  for (const file of globalStale) removed.add(file.path);
  yield* removeEmptyDirectories(state, scan.owners);
  return { root, files: discovered.filter((file) => !removed.has(file.path)) };
});

const pruneForAllocation = Effect.fn("CodeKernelDiagnosticStore.pruneForAllocation")(function* (
  state: StoreState,
  ownerDirectory: string,
  files: ReadonlyArray<StoredFile>,
) {
  const ownerFiles = files.filter((file) => file.ownerDirectory === ownerDirectory);
  const ownerStale = ownerFiles
    .toSorted((left, right) => left.timestamp - right.timestamp)
    .filter((file) => !state.active.has(file.path))
    .slice(0, Math.max(0, ownerFiles.length - state.maxFilesPerOwner + 1));
  yield* removeFiles(state, ownerStale);
  const ownerRemoved = new Set(ownerStale.map((file) => file.path));
  const afterOwner = files.filter((file) => !ownerRemoved.has(file.path));
  const globalStale = afterOwner
    .toSorted((left, right) => left.timestamp - right.timestamp)
    .filter((file) => !state.active.has(file.path))
    .slice(0, Math.max(0, afterOwner.length - state.maxFilesTotal + 1));
  yield* removeFiles(state, globalStale);
  return [...ownerStale, ...globalStale];
});

const allocate = Effect.fn("CodeKernelDiagnosticStore.allocate")(function* (
  state: StoreState,
  owner: AgentOwner,
  pid: number,
) {
  const cleaned = yield* cleanup(state);
  const root = cleaned.root;
  const target = yield* ensureOwnerDirectory(state, root, owner);
  if (target === undefined) return undefined;
  const removed = yield* pruneForAllocation(state, target.directory, cleaned.files);
  const removedPaths = new Set(removed.map((file) => file.path));
  const retained = cleaned.files.filter((file) => !removedPaths.has(file.path));
  const affectedOwners = new Map(
    removed.map((file) => [file.ownerDirectory, file.sessionDirectory]),
  );
  affectedOwners.set(target.directory, target.session);
  yield* removeEmptyDirectories(state, affectedOwners);
  if (retained.length >= state.maxFilesTotal) return undefined;
  if (
    retained.filter((file) => file.ownerDirectory === target.directory).length >=
    state.maxFilesPerOwner
  ) {
    return undefined;
  }
  const recreated = yield* ensureOwnerDirectory(state, root, owner);
  if (recreated === undefined) return undefined;
  const now = yield* Clock.currentTimeMillis;
  const filePath = state.path.join(recreated.directory, `${now}-${pid}.stderr.log`);
  yield* state.fs.writeFile(filePath, new Uint8Array(), { flag: "wx" });
  state.active.add(filePath);
  return { path: filePath, maxFileBytes: state.maxFileBytes } satisfies KernelDiagnosticFile;
});

export const makeCodeKernelDiagnosticStore = Effect.fn("CodeKernelDiagnosticStore.make")(function* (
  config: CodeKernelDiagnosticStoreConfig,
) {
  const directory = config.diagnosticsDirectory;
  if (directory === undefined) return undefined;
  const state: StoreState = {
    fs: yield* FileSystem.FileSystem,
    path: yield* Path.Path,
    directory,
    active: new Set(),
    maxFileBytes: naturalLimit(config.maxFileBytes, 1024 * 1024),
    maxFilesPerOwner: naturalLimit(config.maxFilesPerOwner, 20),
    maxFilesTotal: naturalLimit(config.maxFilesTotal, 256),
  };
  const semaphore = yield* Semaphore.make(1);
  const reserve = (owner: AgentOwner, pid: number) =>
    Effect.acquireRelease(Semaphore.withPermit(semaphore, allocate(state, owner, pid)), (file) =>
      Effect.sync(() => file !== undefined && state.active.delete(file.path)),
    );
  yield* Semaphore.withPermit(semaphore, cleanup(state)).pipe(
    Effect.tapError((error) => Effect.logWarning("Code Kernel diagnostic cleanup failed.", error)),
    Effect.ignore,
  );
  return { reserve } satisfies CodeKernelDiagnosticStore;
});
