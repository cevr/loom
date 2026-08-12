import {
  createEditToolDefinition,
  type EditToolDetails,
  keyHint,
  renderDiff,
} from "@earendil-works/pi-coding-agent";
import { wrapTextWithAnsi } from "@earendil-works/pi-tui";
import { Option } from "effect";
import {
  displayFilePath,
  fileToolResultText,
  fileToolStatus,
  type LoomFileToolTheme,
  renderLoomFileError,
  renderLoomFileHeader,
  renderLoomFileSummary,
} from "./loom-file-tool-ui.js";
import { loomRenderComponent, type LoomToolStatus, renderLoomToolLine } from "./loom-tool-ui.js";

export interface LoomEditRowView {
  readonly path: string;
  readonly status: LoomToolStatus;
  readonly expanded: boolean;
  readonly expandHint: string;
  readonly diff: Option.Option<string>;
  readonly error: Option.Option<string>;
}

const changedLines = (diff: string) => {
  let added = 0;
  let removed = 0;
  for (const line of diff.split("\n")) {
    if (line.startsWith("+")) added += 1;
    else if (line.startsWith("-")) removed += 1;
  }
  return { added, removed };
};

const renderSummary = (view: LoomEditRowView, width: number, theme: LoomFileToolTheme) =>
  Option.match(view.diff, {
    onNone: () => [],
    onSome: (diff) => {
      const counts = changedLines(diff);
      const suffix = `${theme.fg("toolDiffAdded", `+${counts.added}`)} ${theme.fg("toolDiffRemoved", `-${counts.removed}`)}`;
      return [renderLoomFileSummary(view.path, suffix, width, theme)];
    },
  });

const renderBody = (view: LoomEditRowView, width: number, theme: LoomFileToolTheme) => {
  if (view.status === "error") return renderLoomFileError(view.error, width, theme);
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
  theme: LoomFileToolTheme,
): Array<string> => [
  renderLoomToolLine(
    renderLoomFileHeader({ label: "edit", ...view }, theme),
    width,
    view.status,
    theme,
  ),
  ...renderBody(view, width, theme),
];

export const createPrimeEditTool = (cwd: string) => {
  const definition = createEditToolDefinition(cwd);
  const renderPreview = definition.renderCall;
  const tool = {
    ...definition,
    renderCall: (args, theme, context) => {
      if (context.isPartial && renderPreview) return renderPreview(args, theme, context);
      let expandHint = keyHint("app.tools.expand", "to expand");
      if (context.expanded) expandHint = keyHint("app.tools.expand", "to collapse");
      const view: LoomEditRowView = {
        path: displayFilePath(args.path, context.cwd),
        status: fileToolStatus(context),
        expanded: context.expanded,
        expandHint,
        diff: Option.none(),
        error: Option.none(),
      };
      return loomRenderComponent((width) => [
        renderLoomToolLine(
          renderLoomFileHeader({ label: "edit", ...view }, theme),
          width,
          view.status,
          theme,
        ),
      ]);
    },
    renderResult: (result, options, theme, context) => {
      const details: Option.Option<EditToolDetails> = Option.fromNullishOr(result.details);
      let error = Option.none<string>();
      if (context.isError) error = Option.some(fileToolResultText(result.content));
      const view: LoomEditRowView = {
        path: displayFilePath(context.args.path, context.cwd),
        status: fileToolStatus(context),
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
