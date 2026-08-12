import { SessionId } from "@cvr/loom-domain";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Effect, Fiber, Inspectable, MutableRef, Option, Schedule, Stream } from "effect";
import type { LoomExtensionApi } from "./extension-api.js";
import type { LoomEditorState } from "./loom-editor.js";
import {
  LoomDaemonView,
  type LoomInterfaceView,
  modelName,
  renderLoomHeader,
} from "./loom-splash.js";
import { renderLoomTray, trayView } from "./loom-tray.js";
import { type EnsureLoomDaemon, streamWithLoomClient } from "./loom-connection.js";

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
        daemon: LoomDaemonView.cases.Failed.make({ message: Inspectable.toStringUnknown(cause) }),
        actors: [],
      },
      requestRender,
    );
  const attempt = Effect.gen(function* () {
    const daemon = yield* ensureDaemon(context.cwd);
    const ready = LoomDaemonView.cases.Ready.make({
      protocolVersion: daemon.protocolVersion,
      idleLeaseMillis: daemon.codeKernelIdleLeaseMillis,
    });
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

const registerComponents = (
  context: ExtensionContext,
  state: MutableRef.MutableRef<LoomInterfaceView>,
  editorState: LoomEditorState,
  setRequestRender: (render: () => void) => void,
): void => {
  context.ui.setHeader((tui, theme) => {
    setRequestRender(() => tui.requestRender());
    return {
      invalidate() {},
      render: (width) =>
        renderLoomHeader(
          MutableRef.get(state),
          { model: modelName(context), cwd: context.cwd },
          width,
          theme,
        ),
    };
  });
  context.ui.setFooter((_tui, theme, footerData) => ({
    invalidate() {},
    render: (width) =>
      renderLoomTray(
        trayView(context, editorState, MutableRef.get(state), footerData),
        width,
        theme,
      ),
  }));
};

export const registerLoomInterface = (
  pi: LoomExtensionApi,
  ensureDaemon: EnsureLoomDaemon,
  editorState: LoomEditorState,
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
        daemon: LoomDaemonView.cases.Connecting.make({}),
        actors: [],
      });
      let requestRender = () => {};
      registerComponents(context, state, editorState, (render) => {
        requestRender = render;
      });
      fiber = Option.some(
        yield* Effect.forkDetach(runInterface(context, ensureDaemon, state, () => requestRender())),
      );
    }).pipe(Effect.runPromise),
  );

  pi.on("session_shutdown", () => Effect.runPromise(stop()));
};
