import {
  createEditToolDefinition,
  type EditToolDetails,
  keyHint,
  renderDiff,
  type Theme,
} from "@earendil-works/pi-coding-agent";
import { truncateToWidth, visibleWidth, wrapTextWithAnsi } from "@earendil-works/pi-tui";
import { Option } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";
import {
  loomRenderComponent,
  type LoomToolStatus,
  type LoomToolTheme,
  renderLoomToolLine,
} from "./loom-tool-ui.js";

export interface LoomEditRowView {
  readonly path: string;
  readonly status: LoomToolStatus;
  readonly expanded: boolean;
  readonly expandHint: string;
  readonly diff: Option.Option<string>;
  readonly error: Option.Option<string>;
}

type EditTheme = LoomToolTheme & Pick<Theme, "bold">;

const editStatus = (context: {
  readonly isError: boolean;
  readonly executionStarted: boolean;
  readonly isPartial: boolean;
}): LoomToolStatus => {
  if (context.isError) return "error";
  if (!context.executionStarted) return "queued";
  if (context.isPartial) return "running";
  return "done";
};

const displayPath = (path: string, cwd: string) => {
  const workspacePrefix = `${cwd.replace(/\/$/u, "")}/`;
  if (path.startsWith(workspacePrefix)) return path.slice(workspacePrefix.length);
  return path;
};

const changedLines = (diff: string) => {
  let added = 0;
  let removed = 0;
  for (const line of diff.split("\n")) {
    if (line.startsWith("+")) added += 1;
    else if (line.startsWith("-")) removed += 1;
  }
  return { added, removed };
};

const editHeader = (view: LoomEditRowView, theme: EditTheme) => {
  const label = `${theme.fg("toolTitle", theme.bold("edit"))} ${theme.fg("accent", view.path)}`;
  if (view.status !== "done" || view.expandHint.length === 0) return label;
  return `${label}${theme.fg("dim", " · ")}${view.expandHint}`;
};

const renderSummary = (view: LoomEditRowView, width: number, theme: EditTheme) =>
  Option.match(view.diff, {
    onNone: () => [],
    onSome: (diff) => {
      const safeWidth = Math.max(1, width);
      const prefix = theme.fg("dim", "    ╰─ ");
      const counts = changedLines(diff);
      const suffix = `${theme.fg("dim", " ")}${theme.fg("toolDiffAdded", `+${counts.added}`)} ${theme.fg("toolDiffRemoved", `-${counts.removed}`)}`;
      const available = Math.max(1, safeWidth - visibleWidth(prefix) - visibleWidth(suffix));
      const path = truncateToWidth(view.path, available, "…");
      return [truncateToWidth(`${prefix}${theme.fg("muted", path)}${suffix}`, safeWidth, "")];
    },
  });

const renderBody = (view: LoomEditRowView, width: number, theme: EditTheme) => {
  if (view.status === "error") {
    return Option.match(view.error, {
      onNone: () => [],
      onSome: (error) => [
        renderLoomToolLine("", width, view.status, theme),
        ...wrapTextWithAnsi(theme.fg("error", error), Math.max(1, width - 4)).map((line) =>
          renderLoomToolLine(line, width, view.status, theme),
        ),
      ],
    });
  }
  if (!view.expanded) return renderSummary(view, width, theme);
  return Option.match(view.diff, {
    onNone: () => [],
    onSome: (diff) => [
      renderLoomToolLine("", width, view.status, theme),
      ...renderDiff(diff, { filePath: view.path })
        .split("\n")
        .flatMap((line) => wrapTextWithAnsi(line, Math.max(1, width - 4)))
        .map((line) => renderLoomToolLine(line, width, view.status, theme)),
    ],
  });
};

export const renderLoomEditRow = (
  view: LoomEditRowView,
  width: number,
  theme: EditTheme,
): Array<string> => [
  renderLoomToolLine(editHeader(view, theme), width, view.status, theme),
  ...renderBody(view, width, theme),
];

const resultText = (content: ReadonlyArray<{ readonly type: string; readonly text?: string }>) =>
  content
    .filter((item) => item.type === "text")
    .flatMap((item) => Option.toArray(Option.fromNullishOr(item.text)))
    .join("\n");

const primeEditTool = (cwd: string) => {
  const definition = createEditToolDefinition(cwd);
  const renderPreview = definition.renderCall;
  const tool = {
    ...definition,
    renderCall: (args, theme, context) => {
      if (context.isPartial && renderPreview) return renderPreview(args, theme, context);
      let expandHint = keyHint("app.tools.expand", "to expand");
      if (context.expanded) expandHint = keyHint("app.tools.expand", "to collapse");
      const view: LoomEditRowView = {
        path: displayPath(args.path, context.cwd),
        status: editStatus(context),
        expanded: context.expanded,
        expandHint,
        diff: Option.none(),
        error: Option.none(),
      };
      return loomRenderComponent((width) => [
        renderLoomToolLine(editHeader(view, theme), width, view.status, theme),
      ]);
    },
    renderResult: (result, options, theme, context) => {
      const details: Option.Option<EditToolDetails> = Option.fromNullishOr(result.details);
      let error = Option.none<string>();
      if (context.isError) error = Option.some(resultText(result.content));
      const view: LoomEditRowView = {
        path: displayPath(context.args.path, context.cwd),
        status: editStatus(context),
        expanded: options.expanded,
        expandHint: "",
        diff: Option.map(details, (value) => value.diff),
        error,
      };
      return loomRenderComponent((width) => renderBody(view, width, theme));
    },
  } satisfies typeof definition;
  return tool;
};

export const registerPrimeEditTool = (pi: LoomExtensionApi): void => {
  pi.on("session_start", (_event, context) => {
    pi.registerTool(primeEditTool(context.cwd));
  });
};
