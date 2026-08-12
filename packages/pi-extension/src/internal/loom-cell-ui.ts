import {
  generateDiffString,
  keyText,
  renderDiff,
  type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import type { TSchema } from "@earendil-works/pi-ai";
import {
  type Component,
  truncateToWidth,
  visibleWidth,
  wrapTextWithAnsi,
} from "@earendil-works/pi-tui";
import type { CellEvaluation, CellFileChange } from "@cvr/loom-protocol";
import { Option } from "effect";
import {
  displayFilePath,
  renderLoomFileSummary,
  type LoomCellFileTheme,
} from "./loom-cell-file-ui.js";
import {
  cellCallStatus,
  cellResultStatus,
  type LoomCellStatus,
  updateWorkingIndicator,
  workingFrameIndex,
} from "./loom-cell-status.js";

const workingFrames = {
  first: "◇",
  second: "◈",
  third: "◆",
  fourth: "◈",
};

const workingFrame = (frame: number) => {
  switch (frame % 4) {
    case 0:
      return workingFrames.first;
    case 1:
      return workingFrames.second;
    case 2:
      return workingFrames.third;
    default:
      return workingFrames.fourth;
  }
};

export interface LoomCellView {
  readonly source: string;
  readonly status: LoomCellStatus;
  readonly frame: number;
  readonly expanded: boolean;
  readonly showExpandHint: boolean;
  readonly expandHint: string;
  readonly evaluation: Option.Option<CellEvaluation>;
  readonly error: Option.Option<string>;
  readonly cwd: string;
}

type CellTheme = LoomCellFileTheme;
export interface CellRendererState {
  cell?: LoomCellComponent;
  interval?: ReturnType<typeof setInterval>;
  startedAt?: number;
}
type CellRenderContext = Parameters<
  NonNullable<ToolDefinition<TSchema, CellEvaluation, CellRendererState>["renderCall"]>
>[2];

const callPattern = /\bloom\.(read|find|grep|write|edit|run)\s*\(\s*(["'`])([^"'`]*)\2/u;

const preview = (source: string) => {
  const trimmed = source.trim();
  const call = callPattern.exec(trimmed);
  if (call?.[1] && call[3]) {
    if (call[1] === "run") return { language: "bash", text: call[3] };
    return {
      language: "typescript",
      text: `${call[1]} ${call[3]}`,
    };
  }
  const line = trimmed
    .split("\n")
    .map((value) => value.trim())
    .find((value) => value.length > 0 && !value.startsWith("//"));
  return { language: "typescript", text: line ?? "" };
};

const lineCount = (text: string) =>
  text.split("\n").filter((line) => line.trim().length > 0).length;

const marker = (status: LoomCellStatus, frame: number, theme: CellTheme) => {
  switch (status) {
    case "queued":
      return theme.fg("muted", "◇");
    case "running":
      return theme.fg("bashMode", workingFrame(frame));
    case "done":
      return theme.fg("success", "✓");
    case "error":
      return theme.fg("error", "✗");
  }
};

const duration = (milliseconds: number) => {
  if (milliseconds < 1_000) return `${Math.round(milliseconds)}ms`;
  return `${(milliseconds / 1_000).toFixed(1)}s`;
};

const outputLineCount = (view: LoomCellView) =>
  Option.match(view.evaluation, {
    onNone: () => 0,
    onSome: (evaluation) => {
      if (evaluation.fileChanges.length > 0 || evaluation.display === "undefined") return 0;
      return lineCount(evaluation.display);
    },
  });

const collapsedLine = (view: LoomCellView, width: number, theme: CellTheme) => {
  const shown = preview(view.source);
  const parts = [marker(view.status, view.frame, theme), theme.fg("muted", shown.language)];
  if (shown.text) {
    let color: Parameters<CellTheme["fg"]>[0] = "mdCodeBlock";
    if (shown.language === "bash") color = "bashMode";
    parts.push(theme.fg(color, shown.text));
  }
  const input = lineCount(view.source);
  const output = outputLineCount(view);
  const counts = [`↑ ${input}`];
  if (output > 0) counts.push(`↓ ${output}`);
  parts.push(theme.fg("muted", `${counts.join(" ")} lines`));
  if (Option.isSome(view.evaluation)) {
    parts.push(theme.fg("muted", duration(view.evaluation.value.durationMillis)));
  }
  if (view.showExpandHint) {
    let action = "to expand";
    if (view.expanded) action = "to collapse";
    parts.push(theme.fg("dim", `${view.expandHint} ${action}`));
  }
  return truncateToWidth(` ${parts.join(theme.fg("dim", " · "))}`, Math.max(1, width), "");
};

const addWrapped = (
  lines: string[],
  prefix: string,
  text: string,
  width: number,
  theme: CellTheme,
  color: Parameters<CellTheme["fg"]>[0],
) => {
  const available = Math.max(1, width - 1 - visibleWidth(prefix));
  for (const [index, line] of wrapTextWithAnsi(theme.fg(color, text), available).entries()) {
    let shownPrefix = " ".repeat(visibleWidth(prefix));
    if (index === 0) shownPrefix = prefix;
    lines.push(truncateToWidth(` ${shownPrefix}${line}`, width, ""));
  }
};

const changedLineCounts = (change: CellFileChange) => {
  const { diff } = generateDiffString(change.oldText, change.newText, 4);
  let added = 0;
  let removed = 0;
  for (const line of diff.split("\n")) {
    if (line.startsWith("+")) added += 1;
    else if (line.startsWith("-")) removed += 1;
  }
  return { diff, added, removed };
};

const renderChanges = (view: LoomCellView, width: number, theme: CellTheme) =>
  Option.match(view.evaluation, {
    onNone: () => [],
    onSome: (evaluation) =>
      evaluation.fileChanges.flatMap((change) => {
        const { diff, added, removed } = changedLineCounts(change);
        if (!view.expanded) {
          const suffix = `${theme.fg("toolDiffAdded", `+${added}`)} ${theme.fg("toolDiffRemoved", `-${removed}`)}`;
          return [
            renderLoomFileSummary(displayFilePath(change.path, view.cwd), suffix, width, theme),
          ];
        }
        return [
          "",
          ` ${marker(view.status, view.frame, theme)} ${displayFilePath(change.path, view.cwd)}  ${theme.fg("toolDiffAdded", `+${added}`)} ${theme.fg("toolDiffRemoved", `-${removed}`)}`,
          ...renderDiff(diff, { filePath: change.path }).split("\n"),
        ].map((line) => truncateToWidth(line, width, ""));
      }),
  });

export const renderLoomCell = (
  view: LoomCellView,
  width: number,
  theme: CellTheme,
): Array<string> => {
  const safeWidth = Math.max(1, width);
  const lines = [collapsedLine(view, safeWidth, theme)];
  lines.push(...renderChanges(view, safeWidth, theme));
  if (!view.expanded) return lines;
  lines.push("");
  for (const [index, line] of view.source.trimEnd().split("\n").entries()) {
    let prefix = "  ";
    if (index === 0) prefix = "› ";
    addWrapped(lines, prefix, line, safeWidth, theme, "mdCodeBlock");
  }
  if (Option.isSome(view.evaluation)) {
    const evaluation = view.evaluation.value;
    if (evaluation.fileChanges.length === 0 && evaluation.display !== "undefined") {
      lines.push("");
      addWrapped(lines, "  ", evaluation.display, safeWidth, theme, "toolOutput");
    }
  }
  if (Option.isSome(view.error)) {
    lines.push("");
    addWrapped(lines, "  ", view.error.value, safeWidth, theme, "error");
  }
  return lines;
};

class LoomCellComponent implements Component {
  constructor(
    private view: LoomCellView,
    private readonly theme: CellTheme,
  ) {}

  update(view: LoomCellView): void {
    this.view = view;
  }

  render(width: number): Array<string> {
    return renderLoomCell(this.view, width, this.theme);
  }

  invalidate(): void {}
}

export const renderCellCall = (source: string, theme: CellTheme, context: CellRenderContext) => {
  const status = cellCallStatus(context);
  updateWorkingIndicator(context, status);
  const view = {
    source,
    status,
    frame: workingFrameIndex(context, status),
    expanded: context.expanded,
    showExpandHint: true,
    expandHint: keyText("app.tools.expand"),
    evaluation: Option.none<CellEvaluation>(),
    error: Option.none<string>(),
    cwd: context.cwd,
  } satisfies LoomCellView;
  const component = context.state.cell ?? new LoomCellComponent(view, theme);
  component.update(view);
  context.state.cell = component;
  return component;
};

export const renderCellResult = (
  source: string,
  evaluation: Option.Option<CellEvaluation>,
  error: Option.Option<string>,
  expanded: boolean,
  theme: CellTheme,
  context: CellRenderContext,
) => {
  const status = cellResultStatus(context);
  updateWorkingIndicator(context, status);
  const component = context.state.cell;
  component?.update({
    source,
    status,
    frame: workingFrameIndex(context, status),
    expanded,
    showExpandHint: true,
    expandHint: keyText("app.tools.expand"),
    evaluation,
    error,
    cwd: context.cwd,
  });
  return { invalidate() {}, render: () => [] } satisfies Component;
};
