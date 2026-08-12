import type { Theme } from "@earendil-works/pi-coding-agent";
import { truncateToWidth, visibleWidth, wrapTextWithAnsi } from "@earendil-works/pi-tui";
import { Option } from "effect";
import { type LoomToolStatus, type LoomToolTheme, renderLoomToolLine } from "./loom-tool-ui.js";

export type LoomFileToolTheme = LoomToolTheme & Pick<Theme, "bold">;

export interface LoomFileHeaderView {
  readonly label: string;
  readonly path: string;
  readonly status: LoomToolStatus;
  readonly expandHint: string;
}

export const fileToolStatus = (context: {
  readonly isError: boolean;
  readonly executionStarted: boolean;
  readonly isPartial: boolean;
}): LoomToolStatus => {
  if (context.isError) return "error";
  if (!context.executionStarted) return "queued";
  if (context.isPartial) return "running";
  return "done";
};

export const displayFilePath = (path: string, cwd: string) => {
  const workspacePrefix = `${cwd.replace(/\/$/u, "")}/`;
  if (path.startsWith(workspacePrefix)) return path.slice(workspacePrefix.length);
  return path;
};

export const renderLoomFileHeader = (view: LoomFileHeaderView, theme: LoomFileToolTheme) => {
  const label = `${theme.fg("toolTitle", theme.bold(view.label))} ${theme.fg("accent", view.path)}`;
  if (view.status !== "done" || view.expandHint.length === 0) return label;
  return `${label}${theme.fg("dim", " · ")}${view.expandHint}`;
};

export const renderLoomFileSummary = (
  path: string,
  suffix: string,
  width: number,
  theme: LoomFileToolTheme,
) => {
  const safeWidth = Math.max(1, width);
  const prefix = theme.fg("dim", "    ╰─ ");
  const gap = theme.fg("dim", " ");
  const available = Math.max(
    1,
    safeWidth - visibleWidth(prefix) - visibleWidth(gap) - visibleWidth(suffix),
  );
  const shownPath = truncateToWidth(path, available, "…");
  return truncateToWidth(`${prefix}${theme.fg("muted", shownPath)}${gap}${suffix}`, safeWidth, "");
};

export const renderLoomFileError = (
  error: Option.Option<string>,
  width: number,
  theme: LoomFileToolTheme,
): Array<string> =>
  Option.match(error, {
    onNone: () => [],
    onSome: (message) => [
      renderLoomToolLine("", width, "error", theme),
      ...wrapTextWithAnsi(theme.fg("error", message), Math.max(1, width - 4)).map((line) =>
        renderLoomToolLine(line, width, "error", theme),
      ),
    ],
  });

export const fileToolResultText = (
  content: ReadonlyArray<{ readonly type: string; readonly text?: string }>,
) =>
  content
    .filter((item) => item.type === "text")
    .flatMap((item) => Option.toArray(Option.fromNullishOr(item.text)))
    .join("\n");
