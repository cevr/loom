import {
  ActorActivity,
  ActorSubject,
  type ActorStateProjection,
  SessionId,
} from "@cvr/loom-domain";
import type { ExtensionContext, Theme } from "@earendil-works/pi-coding-agent";
import { truncateToWidth } from "@earendil-works/pi-tui";
import {
  Duration,
  Effect,
  Fiber,
  Inspectable,
  Match,
  MutableRef,
  Option,
  Schedule,
  Stream,
} from "effect";
import type { LoomExtensionApi } from "./extension-api.js";
import { type EnsureLoomDaemon, streamWithLoomClient } from "./loom-connection.js";

export type LoomDaemonView =
  | { readonly _tag: "Connecting" }
  | {
      readonly _tag: "Ready";
      readonly idleLeaseMillis: number;
    }
  | { readonly _tag: "Failed"; readonly message: string };

export interface LoomInterfaceView {
  readonly daemon: LoomDaemonView;
  readonly actors: ReadonlyArray<ActorStateProjection>;
}

type LoomTheme = Pick<Theme, "bold" | "fg">;

const daemonText = Match.type<LoomDaemonView>().pipe(
  Match.tagsExhaustive({
    Connecting: () => "daemon connecting",
    Failed: ({ message }) => `daemon failed: ${message.replaceAll(/\s+/gu, " ").trim()}`,
    Ready: ({ idleLeaseMillis }) =>
      `daemon ready · lease ${Duration.format(Duration.millis(idleLeaseMillis))}`,
  }),
);

const workingText = (message: Option.Option<string>, detailed: boolean) => {
  if (!detailed) return "working";
  return Option.match(message, {
    onNone: () => "working",
    onSome: (detail) => `working: ${detail}`,
  });
};

const activityText = (activity: ActorActivity, detailed: boolean) =>
  ActorActivity.match(activity, {
    Idle: () => "idle",
    Working: ({ message }) => workingText(Option.fromNullishOr(message), detailed),
    Blocked: ({ message }) => `blocked: ${message}`,
    Failed: ({ message }) => `failed: ${message}`,
    Stopped: () => "stopped",
  });

const compactId = (id: string) => {
  if (id.length <= 8) return id;
  return id.slice(-8);
};

const actorIdentity = (wide: boolean, id: string) => {
  if (!wide) return "";
  return ` ${compactId(id)}`;
};

const actorText = (projection: ActorStateProjection, wide: boolean) =>
  ActorSubject.match(projection.subject, {
    Agent: () => `actor ${activityText(projection.activity, wide)}`,
    Job: ({ jobId }) =>
      `job${actorIdentity(wide, jobId)} ${activityText(projection.activity, wide)}`,
    WorkflowRun: ({ workflowRunId }) =>
      `workflow${actorIdentity(wide, workflowRunId)} ${activityText(projection.activity, wide)}`,
  });

export const renderLoomHeader = (
  view: LoomInterfaceView,
  width: number,
  theme: LoomTheme,
): ReadonlyArray<string> => {
  const safeWidth = Math.max(1, width);
  const heading = `${theme.bold(theme.fg("accent", "loom"))} ${theme.fg("dim", `· ${daemonText(view.daemon)}`)}`;
  const lines = [truncateToWidth(heading, safeWidth, "…")];
  if (view.actors.length === 0) return lines;
  const activity = view.actors.map((actor) => actorText(actor, safeWidth >= 72)).join(" · ");
  lines.push(truncateToWidth(theme.fg("muted", activity), safeWidth, "…"));
  return lines;
};

const setInterface = (
  state: MutableRef.MutableRef<LoomInterfaceView>,
  view: LoomInterfaceView,
  requestRender: () => void,
) =>
  Effect.sync(() => {
    MutableRef.set(state, view);
    requestRender();
  });

const runInterface = (
  context: ExtensionContext,
  ensureDaemon: EnsureLoomDaemon,
  state: MutableRef.MutableRef<LoomInterfaceView>,
  requestRender: () => void,
) => {
  const showFailure = (cause: unknown) =>
    setInterface(
      state,
      {
        daemon: { _tag: "Failed", message: Inspectable.toStringUnknown(cause) },
        actors: [],
      },
      requestRender,
    );
  const attempt = Effect.gen(function* () {
    const daemon = yield* ensureDaemon(context.cwd);
    const ready = {
      _tag: "Ready",
      idleLeaseMillis: daemon.codeKernelIdleLeaseMillis,
    } satisfies LoomDaemonView;
    yield* setInterface(state, { daemon: ready, actors: [] }, requestRender);
    if (daemon.started) {
      yield* Effect.sync(() => context.ui.notify("Loom daemon started.", "info"));
    }
    const sessionId = SessionId.make(context.sessionManager.getSessionId());
    yield* streamWithLoomClient(context.cwd, (client) => client.watchActorStates(sessionId)).pipe(
      Stream.runForEach((actors) => setInterface(state, { daemon: ready, actors }, requestRender)),
    );
  });
  return attempt.pipe(
    Effect.tapError(showFailure),
    Effect.retry(Schedule.spaced("1 second")),
    Effect.catchCause(showFailure),
  );
};

export const registerLoomInterface = (
  pi: LoomExtensionApi,
  ensureDaemon: EnsureLoomDaemon,
): void => {
  let fiber = Option.none<Fiber.Fiber<void>>();
  const stop = () =>
    Option.match(fiber, {
      onNone: () => Effect.void,
      onSome: (running) => Fiber.interrupt(running).pipe(Effect.asVoid),
    });

  pi.on("session_start", (_event, context) =>
    Effect.gen(function* () {
      yield* stop();
      const state = MutableRef.make<LoomInterfaceView>({
        daemon: { _tag: "Connecting" },
        actors: [],
      });
      let requestRender = () => {};
      context.ui.setHeader((tui, theme) => {
        requestRender = () => tui.requestRender();
        return {
          invalidate() {},
          render: (width) => [...renderLoomHeader(MutableRef.get(state), width, theme)],
        };
      });
      context.ui.setFooter(() => ({ invalidate() {}, render: () => [] }));
      fiber = Option.some(
        yield* Effect.forkDetach(runInterface(context, ensureDaemon, state, requestRender)),
      );
    }).pipe(Effect.runPromise),
  );

  pi.on("session_shutdown", () => Effect.runPromise(stop()));
};
