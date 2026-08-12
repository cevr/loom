import {
  type AgentToolResult,
  keyHint,
  type Theme,
  type ToolDefinition,
} from "@earendil-works/pi-coding-agent";
import type { TSchema } from "@earendil-works/pi-ai";
import {
  type Component,
  truncateToWidth,
  visibleWidth,
  wrapTextWithAnsi,
} from "@earendil-works/pi-tui";
import { Inspectable, Option } from "effect";

export const loomWorkingFrames = ["◇", "◈", "◆", "◈"] satisfies ReadonlyArray<string>;

export type LoomToolStatus = "queued" | "running" | "done" | "error";

export interface LoomToolPanelView {
  readonly label: string;
  readonly status: LoomToolStatus;
  readonly input: string;
  readonly output: Option.Option<string>;
  readonly frame: number;
  readonly expanded: boolean;
  readonly expandHint: string;
}

interface LoomToolState {
  interval?: ReturnType<typeof setInterval>;
  startedAt?: number;
}

export type LoomToolTheme = Pick<Theme, "bg" | "fg">;
type LoomToolRenderContext = Parameters<
  NonNullable<ToolDefinition<TSchema, unknown, LoomToolState>["renderCall"]>
>[2];
type Render = Component["render"];

const panelPadding = 2;
const maximumPreviewLines = 12;
const workingFrameMillis = 250;

export const loomRenderComponent = (render: Render): Component => ({ invalidate() {}, render });

const panelStatus = (view: LoomToolPanelView, theme: LoomToolTheme) => {
  switch (view.status) {
    case "queued":
      return theme.fg("muted", "queued");
    case "running": {
      const frame = loomWorkingFrames[view.frame % loomWorkingFrames.length];
      return theme.fg("bashMode", `${frame} running`);
    }
    case "done":
      return theme.fg("success", "done");
    case "error":
      return theme.fg("error", "error");
  }
};

const withPanelBackground = (status: LoomToolStatus, text: string, theme: LoomToolTheme) => {
  switch (status) {
    case "queued":
    case "running":
      return theme.bg("toolPendingBg", text);
    case "done":
      return theme.bg("toolSuccessBg", text);
    case "error":
      return theme.bg("toolErrorBg", text);
  }
};

const contentWidth = (width: number) =>
  Math.max(1, width - Math.min(panelPadding, Math.floor((width - 1) / 2)) * 2);

const boundedContent = (
  text: string,
  width: number,
  theme: LoomToolTheme,
  expanded: boolean,
  expandHint: string,
) => {
  const lines = wrapTextWithAnsi(text, width);
  if (expanded || lines.length <= maximumPreviewLines) return lines;
  const hidden = lines.length - maximumPreviewLines;
  return [
    ...lines.slice(0, maximumPreviewLines),
    `${theme.fg("muted", `... (${hidden} more lines, `)}${expandHint}${theme.fg("muted", ")")}`,
  ];
};

export const renderLoomToolLine = (
  line: string,
  width: number,
  status: LoomToolStatus,
  theme: LoomToolTheme,
) => {
  const safeWidth = Math.max(1, width);
  const innerWidth = contentWidth(safeWidth);
  const shown = truncateToWidth(line, innerWidth, "…");
  const edge = " ".repeat((safeWidth - innerWidth) / 2);
  return withPanelBackground(
    status,
    `${edge}${shown}${" ".repeat(Math.max(0, innerWidth - visibleWidth(shown)))}${edge}`,
    theme,
  );
};

const renderCall = (view: LoomToolPanelView, width: number, theme: LoomToolTheme) => {
  const safeWidth = Math.max(1, width);
  const header = `${theme.fg("muted", view.label)}${theme.fg("dim", " · ")}${panelStatus(view, theme)}`;
  return [
    renderLoomToolLine(header, safeWidth, view.status, theme),
    renderLoomToolLine("", safeWidth, view.status, theme),
    ...boundedContent(view.input, contentWidth(safeWidth), theme, false, view.expandHint).map(
      (line) => renderLoomToolLine(line, safeWidth, view.status, theme),
    ),
  ];
};

const renderResult = (
  output: string,
  status: LoomToolStatus,
  width: number,
  theme: LoomToolTheme,
  expanded: boolean,
  expandHint: string,
) => {
  const safeWidth = Math.max(1, width);
  return [
    renderLoomToolLine("", safeWidth, status, theme),
    ...boundedContent(output, contentWidth(safeWidth), theme, expanded, expandHint).map((line) =>
      renderLoomToolLine(line, safeWidth, status, theme),
    ),
  ];
};

export const renderLoomToolPanel = (
  view: LoomToolPanelView,
  width: number,
  theme: LoomToolTheme,
): Array<string> => [
  ...renderCall(view, width, theme),
  ...Option.match(view.output, {
    onNone: () => [],
    onSome: (output) =>
      renderResult(output, view.status, width, theme, view.expanded, view.expandHint),
  }),
];

const toolOutput = (result: AgentToolResult<unknown>) => {
  const lines: string[] = [];
  for (const content of result.content) {
    if (content.type === "text") lines.push(content.text);
    else if (content.type === "image") lines.push(`[image ${content.mimeType}]`);
  }
  return lines.join("\n");
};

const toolStatus = (context: LoomToolRenderContext): LoomToolStatus => {
  if (context.isError) return "error";
  if (!context.executionStarted) return "queued";
  if (context.isPartial) return "running";
  return "done";
};

const updateWorkingIndicator = (context: LoomToolRenderContext) => {
  const running = toolStatus(context) === "running";
  if (running) {
    // Pi renderers expose synchronous invalidation instead of an Effect runtime.
    // oxlint-disable-next-line effect/noGlobals
    context.state.startedAt ??= Date.now();
    // Pi uses this timer pattern for its Bash renderer.
    // oxlint-disable-next-line effect/noGlobals
    context.state.interval ??= setInterval(context.invalidate, workingFrameMillis);
  } else if (context.state.interval) {
    clearInterval(context.state.interval);
    delete context.state.interval;
  }
};

export const loomTool = <TParameters extends TSchema, TDetails>(
  definition: ToolDefinition<TParameters, TDetails, LoomToolState>,
): ToolDefinition<TParameters, TDetails, LoomToolState> => ({
  ...definition,
  renderShell: "self",
  renderCall: (args, theme, context) => {
    updateWorkingIndicator(context);
    const status = toolStatus(context);
    let frame = 0;
    const startedAt = context.state.startedAt;
    if (status === "running" && startedAt) {
      // Pi calls renderers outside an Effect runtime.
      // oxlint-disable-next-line effect/noGlobals
      frame = Math.floor((Date.now() - startedAt) / workingFrameMillis);
    }
    const view: LoomToolPanelView = {
      label: definition.label,
      status,
      input: Inspectable.toStringUnknown(args),
      output: Option.none(),
      frame,
      expanded: context.expanded,
      expandHint: "",
    };
    return loomRenderComponent((width) => renderCall(view, width, theme));
  },
  renderResult: (result, options, theme, context) => {
    const output = toolOutput(result);
    const status = toolStatus(context);
    const expandHint = keyHint("app.tools.expand", "to expand");
    return loomRenderComponent((width) =>
      renderResult(output, status, width, theme, options.expanded, expandHint),
    );
  },
});
