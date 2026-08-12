import type { Theme } from "@earendil-works/pi-coding-agent";
import { truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import { Option } from "effect";

export type LoomCellFileTheme = Pick<Theme, "bold" | "fg">;

export const displayFilePath = (path: string, cwd: string) => {
  const workspacePrefix = `${cwd.replace(/\/$/u, "")}/`;
  if (path.startsWith(workspacePrefix)) return path.slice(workspacePrefix.length);
  return path;
};

export const renderLoomFileSummary = (
  path: string,
  suffix: string,
  width: number,
  theme: LoomCellFileTheme,
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

export const cellResultText = (
  content: ReadonlyArray<{ readonly type: string; readonly text?: string }>,
) =>
  content
    .filter((item) => item.type === "text")
    .flatMap((item) => Option.toArray(Option.fromNullishOr(item.text)))
    .join("\n");
