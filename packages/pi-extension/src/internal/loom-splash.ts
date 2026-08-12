import type { ActorStateProjection } from "@cvr/loom-domain";
import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import { truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import { Duration, Option, Schema } from "effect";

export const LoomDaemonView = Schema.TaggedUnion({
  Connecting: {},
  Ready: {
    protocolVersion: Schema.Finite,
    idleLeaseMillis: Schema.Finite,
  },
  Failed: { message: Schema.String },
});
export type LoomDaemonView = typeof LoomDaemonView.Type;

export interface LoomInterfaceView {
  readonly daemon: LoomDaemonView;
  readonly actors: ReadonlyArray<ActorStateProjection>;
}

export interface LoomHeaderDetails {
  readonly model: string;
  readonly cwd: string;
}

export type LoomTheme = Pick<Theme, "fg">;
type LoomHeaderTheme = LoomTheme & Pick<Theme, "bold">;

const LOOM_LOGO = [
  "  ╭─╮ ╭─╮  ",
  "╭─┤ │ │ ├─╮",
  "│ │ │ │ │ │",
  "╰─┤ ╰─╯ ├─╯",
  "  ╰─────╯  ",
  "",
] satisfies ReadonlyArray<string>;
const LOGO_WIDTH = Math.max(...LOOM_LOGO.map(visibleWidth));
const METADATA_LABEL_WIDTH = 9;

const daemonText = LoomDaemonView.match({
  Connecting: () => "daemon connecting",
  Failed: ({ message }) => `daemon failed: ${message.replaceAll(/\s+/gu, " ").trim()}`,
  Ready: ({ idleLeaseMillis }) =>
    `daemon ready · lease ${Duration.format(Duration.millis(idleLeaseMillis))}`,
});

const compactPath = (path: string, width: number) => {
  if (visibleWidth(path) <= width) return path;
  const parts = path.replaceAll("\\", "/").split("/").filter(Boolean);
  const tail = parts.slice(-2).join("/");
  return truncateToWidth(`…/${tail}`, width, "…");
};

const daemonMetadata = LoomDaemonView.match({
  Connecting: () => [
    { label: "status", value: "connecting" },
    { label: "protocol", value: "—" },
    { label: "lease", value: "—" },
  ],
  Failed: () => [
    { label: "status", value: "failed" },
    { label: "protocol", value: "—" },
    { label: "lease", value: "—" },
  ],
  Ready: ({ idleLeaseMillis, protocolVersion }) => [
    { label: "status", value: "ready" },
    { label: "protocol", value: `v${protocolVersion}` },
    { label: "lease", value: Duration.format(Duration.millis(idleLeaseMillis)) },
  ],
});

export const modelName = (context: ExtensionContext) =>
  Option.match(Option.fromNullishOr(context.model), {
    onNone: () => "No model",
    onSome: (model) => model.name,
  });

export const renderLoomHeader = (
  view: LoomInterfaceView,
  details: LoomHeaderDetails,
  width: number,
  theme: LoomHeaderTheme,
): Array<string> => {
  const safeWidth = Math.max(1, width);
  const paddingX = Math.min(1, safeWidth - 1);
  const contentWidth = Math.max(1, safeWidth - paddingX * 2);
  const gutter = 4;
  const metadataWidth = contentWidth - LOGO_WIDTH - gutter;
  if (metadataWidth < METADATA_LABEL_WIDTH + 8) {
    const heading = `${theme.bold(theme.fg("accent", "loom"))} ${theme.fg("dim", `· ${daemonText(view.daemon)}`)}`;
    return [truncateToWidth(heading, safeWidth, "…")];
  }

  const valueWidth = Math.max(1, metadataWidth - METADATA_LABEL_WIDTH);
  const labelled = (label: string, value: string, compact = false) => {
    if (label.length === 0) return theme.fg("dim", value);
    let shown = truncateToWidth(value, valueWidth);
    if (compact) shown = compactPath(value, valueWidth);
    return theme.fg("dim", label.padEnd(METADATA_LABEL_WIDTH)) + theme.fg("muted", shown);
  };
  const metadata = [
    ...daemonMetadata(view.daemon),
    { label: "model", value: details.model },
    { label: "cwd", value: details.cwd, compact: true },
    { label: "", value: 'Try "build and test @<filepath>"' },
  ].map((item) => labelled(item.label, item.value, "compact" in item && item.compact));

  return LOOM_LOGO.map((line, index) => {
    const logo = theme.bold(theme.fg("accent", line));
    const gap = " ".repeat(LOGO_WIDTH - visibleWidth(line) + gutter);
    const item = Option.getOrElse(Option.fromNullishOr(metadata[index]), () => "");
    const content = truncateToWidth(`${logo}${gap}${item}`, contentWidth, "");
    return `${" ".repeat(paddingX)}${content}${" ".repeat(Math.max(0, safeWidth - paddingX - visibleWidth(content)))}`;
  });
};
