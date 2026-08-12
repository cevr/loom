/* oxlint-disable effect/noAsyncFunction, effect/noGlobals, effect/noNewError, effect/noNodeBuiltinImport, effect/noThrowStatement -- The VM host API is Promise-based and this named Bun adapter translates file failures into rejected Promises. */
import type { WorkspaceRoot } from "@cvr/loom-domain";
import { Option } from "effect";
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";

export interface KernelFileChange {
  readonly path: string;
  readonly oldText: string;
  readonly newText: string;
}

const readOptionalText = async (path: string) => {
  const file = Bun.file(path);
  if (!(await file.exists())) return Option.none<string>();
  return Option.some(await file.text());
};

const readText = async (path: string) => {
  const text = await readOptionalText(path);
  return Option.getOrThrowWith(text, () => new Error(`File does not exist: ${path}`));
};

const visibleLines = (text: string, offset: number, limit: number) =>
  text
    .split("\n")
    .slice(Math.max(0, offset), Math.max(0, offset) + Math.max(1, limit))
    .map((line, index) => `${String(offset + index + 1).padStart(4)} ${line}`)
    .join("\n");

const findFiles = async (workspaceRoot: WorkspaceRoot, pattern: string) => {
  const paths: string[] = [];
  for await (const path of new Bun.Glob(pattern).scan({ cwd: workspaceRoot, onlyFiles: true })) {
    paths.push(path);
    if (paths.length >= 1_000) break;
  }
  return paths.toSorted();
};

const grepFiles = async (workspaceRoot: WorkspaceRoot, pattern: string, globPattern: string) => {
  const matches: Array<{ readonly path: string; readonly line: number; readonly text: string }> =
    [];
  for (const path of await findFiles(workspaceRoot, globPattern)) {
    // A bounded sequential scan avoids opening every matching file at once.
    // oxlint-disable-next-line eslint/no-await-in-loop
    const lines = (await readText(resolve(workspaceRoot, path))).split("\n");
    for (const [index, line] of lines.entries()) {
      if (line.includes(pattern)) matches.push({ path, line: index + 1, text: line });
      if (matches.length >= 1_000) return matches;
    }
  }
  return matches;
};

export const makeKernelFileControls = (workspaceRoot: WorkspaceRoot) => {
  const changes: KernelFileChange[] = [];
  const absolutePath = (path: string) => resolve(workspaceRoot, path);

  const read = async (path: string, options: { offset?: number; limit?: number } = {}) =>
    visibleLines(await readText(absolutePath(path)), options.offset ?? 0, options.limit ?? 200);

  const find = (pattern = "**/*") => findFiles(workspaceRoot, pattern);
  const grep = (pattern: string, globPattern = "**/*") =>
    grepFiles(workspaceRoot, pattern, globPattern);

  const write = async (path: string, content: string) => {
    const absolute = absolutePath(path);
    const oldText = Option.getOrElse(await readOptionalText(absolute), () => "");
    await mkdir(dirname(absolute), { recursive: true });
    await Bun.write(absolute, content);
    changes.push({ path, oldText, newText: content });
    return `Wrote ${path}`;
  };

  const edit = async (path: string, oldText: string, newText: string) => {
    const absolute = absolutePath(path);
    const content = await readText(absolute);
    const first = content.indexOf(oldText);
    if (first < 0) throw new Error(`Text was not found in ${path}`);
    if (content.indexOf(oldText, first + oldText.length) >= 0) {
      throw new Error(`Text occurs more than once in ${path}`);
    }
    const updated = `${content.slice(0, first)}${newText}${content.slice(first + oldText.length)}`;
    await Bun.write(absolute, updated);
    changes.push({ path, oldText: content, newText: updated });
    return `Edited ${path}`;
  };

  return {
    api: { read, find, grep, write, edit },
    beginCell: () => {
      changes.length = 0;
    },
    changes: () => changes,
  };
};
