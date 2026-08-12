import { createWriteToolDefinition, keyHint } from "@earendil-works/pi-coding-agent";
import { Text } from "@earendil-works/pi-tui";
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

export interface LoomWriteRowView {
  readonly path: string;
  readonly status: LoomToolStatus;
  readonly lineCount: number;
  readonly expandHint: string;
  readonly error: Option.Option<string>;
}

interface CollapsedWriteRow {
  readonly header: string;
  readonly status: LoomToolStatus;
  readonly theme: LoomFileToolTheme;
}

class WriteRowComponent extends Text {
  private collapsed = Option.none<CollapsedWriteRow>();

  setCollapsed(row: CollapsedWriteRow): void {
    this.collapsed = Option.some(row);
    super.setText("");
  }

  override setText(text: string): void {
    this.collapsed = Option.none();
    super.setText(text);
  }

  override render(width: number): Array<string> {
    if (Option.isSome(this.collapsed)) {
      const row = this.collapsed.value;
      return [renderLoomToolLine(row.header, width, row.status, row.theme)];
    }
    return super.render(width);
  }
}

const writtenLineCount = (content: string) => {
  const lines = content.split("\n");
  let count = lines.length;
  while (count > 0 && lines[count - 1] === "") count -= 1;
  return count;
};

const renderLoomWriteBody = (
  view: LoomWriteRowView,
  width: number,
  theme: LoomFileToolTheme,
): Array<string> => {
  if (view.status === "error") return renderLoomFileError(view.error, width, theme);
  let unit = "lines";
  if (view.lineCount === 1) unit = "line";
  const suffix = theme.fg("muted", `${view.lineCount} ${unit} written`);
  return [renderLoomFileSummary(view.path, suffix, width, theme)];
};

export const renderLoomWriteRow = (
  view: LoomWriteRowView,
  width: number,
  theme: LoomFileToolTheme,
): Array<string> => [
  renderLoomToolLine(
    renderLoomFileHeader(
      { label: "write", path: view.path, status: view.status, expandHint: view.expandHint },
      theme,
    ),
    width,
    view.status,
    theme,
  ),
  ...renderLoomWriteBody(view, width, theme),
];

type WriteToolDefinition = ReturnType<typeof createWriteToolDefinition>;
type WriteCallRenderer = NonNullable<WriteToolDefinition["renderCall"]>;
type WriteResultRenderer = NonNullable<WriteToolDefinition["renderResult"]>;

const writeCallRenderer =
  (renderPreview: WriteToolDefinition["renderCall"]): WriteCallRenderer =>
  (args, theme, context) => {
    if (context.isPartial && renderPreview) return renderPreview(args, theme, context);
    if (context.expanded && renderPreview) return renderPreview(args, theme, context);
    const view: LoomWriteRowView = {
      path: displayFilePath(args.path, context.cwd),
      status: fileToolStatus(context),
      lineCount: writtenLineCount(args.content),
      expandHint: keyHint("app.tools.expand", "to expand"),
      error: Option.none(),
    };
    let component = new WriteRowComponent("", 0, 0);
    if (context.lastComponent instanceof WriteRowComponent) component = context.lastComponent;
    component.setCollapsed({
      header: renderLoomFileHeader(
        { label: "write", path: view.path, status: view.status, expandHint: view.expandHint },
        theme,
      ),
      status: view.status,
      theme,
    });
    return component;
  };

const writeResultRenderer: WriteResultRenderer = (result, options, theme, context) => {
  let error = Option.none<string>();
  if (context.isError) error = Option.some(fileToolResultText(result.content));
  if (options.expanded && Option.isNone(error)) return loomRenderComponent(() => []);
  const view: LoomWriteRowView = {
    path: displayFilePath(context.args.path, context.cwd),
    status: fileToolStatus(context),
    lineCount: writtenLineCount(context.args.content),
    expandHint: "",
    error,
  };
  return loomRenderComponent((width) => renderLoomWriteBody(view, width, theme));
};

export const createPrimeWriteTool = (cwd: string) => {
  const definition = createWriteToolDefinition(cwd);
  const tool = {
    ...definition,
    renderShell: "self",
    renderCall: writeCallRenderer(definition.renderCall),
    renderResult: writeResultRenderer,
  } satisfies WriteToolDefinition;
  return tool;
};
