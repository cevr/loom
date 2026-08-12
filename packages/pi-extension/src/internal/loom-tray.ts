import { ActorActivity, ActorSubject, type ActorStateProjection } from "@cvr/loom-domain";
import type { ExtensionContext, ReadonlyFooterDataProvider } from "@earendil-works/pi-coding-agent";
import { truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import { MutableRef, Option } from "effect";
import { goalStatusKey } from "./goal-state.js";
import type { LoomEditorState } from "./loom-editor.js";
import { type LoomInterfaceView, type LoomTheme, modelName } from "./loom-splash.js";

export interface LoomTrayView {
  readonly model: string;
  readonly thinkingLevel: Option.Option<string>;
  readonly showShortcutHint: boolean;
  readonly goal: Option.Option<string>;
  readonly usage: Option.Option<{ readonly tokens: number; readonly percent: number }>;
  readonly actors: ReadonlyArray<ActorStateProjection>;
}

const activityText = (activity: ActorActivity, detailed: boolean) =>
  ActorActivity.match(activity, {
    Idle: () => "idle",
    Working: ({ message }) => {
      if (!detailed) return "working";
      return Option.match(Option.fromNullishOr(message), {
        onNone: () => "working",
        onSome: (detail) => `working: ${detail}`,
      });
    },
    Blocked: ({ message }) => `blocked: ${message}`,
    Failed: ({ message }) => `failed: ${message}`,
    Stopped: () => "stopped",
  });

const actorIdentity = (wide: boolean, id: string) => {
  if (!wide) return "";
  if (id.length <= 8) return ` ${id}`;
  return ` ${id.slice(-8)}`;
};

const actorText = (projection: ActorStateProjection, wide: boolean) =>
  ActorSubject.match(projection.subject, {
    Agent: () => `actor ${activityText(projection.activity, wide)}`,
    Job: ({ jobId }) =>
      `job${actorIdentity(wide, jobId)} ${activityText(projection.activity, wide)}`,
    WorkflowRun: ({ workflowRunId }) =>
      `workflow${actorIdentity(wide, workflowRunId)} ${activityText(projection.activity, wide)}`,
  });

const activeActor = (projection: ActorStateProjection) =>
  ActorActivity.match(projection.activity, {
    Idle: () => false,
    Working: () => true,
    Blocked: () => true,
    Failed: () => true,
    Stopped: () => false,
  });

const formatTokenCount = (count: number): string => {
  if (count < 1_000) return count.toString();
  if (count < 10_000) return `${(count / 1_000).toFixed(1)}k`;
  if (count < 1_000_000) return `${Math.round(count / 1_000)}k`;
  if (count < 10_000_000) return `${(count / 1_000_000).toFixed(1)}M`;
  return `${Math.round(count / 1_000_000)}M`;
};

const renderTrayInfo = (left: string, right: string, width: number) => {
  const safeWidth = Math.max(1, width);
  let gap = 0;
  if (left.length > 0 && right.length > 0) gap = 2;
  const rightWidth = Math.min(visibleWidth(right), Math.max(0, safeWidth - gap));
  const leftWidth = Math.max(0, safeWidth - rightWidth - gap);
  const shownLeft = truncateToWidth(left, leftWidth, "…");
  const shownRight = truncateToWidth(right, rightWidth, "…");
  const padding = Math.max(0, safeWidth - visibleWidth(shownLeft) - visibleWidth(shownRight));
  return `${shownLeft}${" ".repeat(padding)}${shownRight}`;
};

export const renderLoomTray = (
  view: LoomTrayView,
  width: number,
  theme: LoomTheme,
): Array<string> => {
  const thinking = Option.match(view.thinkingLevel, {
    onNone: () => [],
    onSome: (level) => [level],
  });
  const shortcut: string[] = [];
  if (view.showShortcutHint) shortcut.push("? for shortcuts");
  const left = [view.model, ...thinking, ...shortcut].join(" • ");
  const goal = Option.match(view.goal, { onNone: () => [], onSome: (status) => [status] });
  const usage = Option.match(view.usage, {
    onNone: () => [],
    onSome: ({ percent, tokens }) => [`${formatTokenCount(tokens)} (${Math.round(percent)}%)`],
  });
  const right = [...goal, ...usage].join(" · ");
  const lines = [theme.fg("muted", renderTrayInfo(left, right, width))];
  const actors = view.actors.filter(activeActor);
  if (actors.length === 0) return lines;
  const activity = actors.map((actor) => actorText(actor, width >= 72)).join(" · ");
  lines.push(theme.fg("dim", truncateToWidth(` ${activity}`, Math.max(1, width), "…")));
  return lines;
};

const contextUsage = (context: ExtensionContext) =>
  Option.gen(function* () {
    const usage = yield* Option.fromNullishOr(context.getContextUsage());
    const tokens = yield* Option.fromNullishOr(usage.tokens);
    const percent = yield* Option.fromNullishOr(usage.percent);
    return { tokens, percent };
  });

const sessionHasMessages = (context: ExtensionContext) =>
  context.sessionManager.getBranch().some((entry) => entry.type === "message");

export const trayView = (
  context: ExtensionContext,
  editorState: LoomEditorState,
  interfaceView: LoomInterfaceView,
  footerData: ReadonlyFooterDataProvider,
): LoomTrayView => ({
  model: modelName(context),
  thinkingLevel: Option.filter(
    Option.fromNullishOr(context.thinkingLevel),
    (level) => level !== "off",
  ),
  showShortcutHint: MutableRef.get(editorState.inputEmpty) && !sessionHasMessages(context),
  goal: Option.fromNullishOr(footerData.getExtensionStatuses().get(goalStatusKey)),
  usage: contextUsage(context),
  actors: interfaceView.actors,
});
