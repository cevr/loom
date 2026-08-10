import { SessionId } from "@cvr/loom-domain";
import type { ExtensionContext, SessionShutdownEvent } from "@earendil-works/pi-coding-agent";
import { Effect } from "effect";
import { registerCellTools } from "./internal/cell-tools.js";
import type { LoomExtensionApi } from "./internal/extension-api.js";
import {
  ensureLoomDaemon,
  type EnsureLoomDaemon,
  type LoomDaemonStatus,
  runWithLoomClient,
} from "./internal/loom-connection.js";
import { registerWorkflowTools } from "./internal/workflow-tools.js";

export { ensureLoomDaemon };
export type { EnsureLoomDaemon, LoomDaemonStatus, LoomExtensionApi };

const notifyFailure = (context: ExtensionContext, cause: unknown) =>
  Effect.sync(() => context.ui.notify(`Loom daemon failed: ${String(cause)}`, "error"));

const ensureForSession = (context: ExtensionContext, ensureDaemon: EnsureLoomDaemon) =>
  ensureDaemon(context.cwd).pipe(
    Effect.matchEffect({
      onFailure: (cause) => notifyFailure(context, cause),
      onSuccess: (status) => {
        if (!status.started) return Effect.void;
        return Effect.sync(() => context.ui.notify("Loom daemon started.", "info"));
      },
    }),
    Effect.runPromise,
  );

export const shouldCloseSession = (event: SessionShutdownEvent): boolean =>
  event.reason !== "reload";

const registerSessionLifecycle = (pi: LoomExtensionApi, ensureDaemon: EnsureLoomDaemon) => {
  pi.on("session_start", (_event, context) => ensureForSession(context, ensureDaemon));
  pi.on("session_shutdown", (event, context) => {
    if (!shouldCloseSession(event)) return;
    return runWithLoomClient(context.cwd, "5 seconds", (client) =>
      client.closeSession(SessionId.make(context.sessionManager.getSessionId())),
    ).pipe(
      Effect.catchCause((cause) => notifyFailure(context, cause)),
      Effect.runPromise,
    );
  });
};

export const registerLoomExtension = (
  pi: LoomExtensionApi,
  ensureDaemon: EnsureLoomDaemon = ensureLoomDaemon,
): void => {
  registerSessionLifecycle(pi, ensureDaemon);
  pi.registerCommand("loom", {
    description: "Show the Loom daemon state",
    handler: (_arguments, context) =>
      ensureDaemon(context.cwd).pipe(
        Effect.matchEffect({
          onFailure: (cause) => notifyFailure(context, cause),
          onSuccess: (status) =>
            Effect.sync(() =>
              context.ui.notify(
                `Loom daemon ready. Protocol ${status.protocolVersion}. Socket ${status.socketPath}`,
                "info",
              ),
            ),
        }),
        Effect.runPromise,
      ),
  });
  registerCellTools(pi);
  registerWorkflowTools(pi);
};

export default function loomExtension(pi: LoomExtensionApi): void {
  registerLoomExtension(pi);
}
