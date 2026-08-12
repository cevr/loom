import type { DaemonUnavailableError, LoomClientShape } from "@cvr/loom-client";
import { SessionId } from "@cvr/loom-domain";
import type { AgentToolResult, ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { Duration, Effect } from "effect";
import { runWithLoomClient } from "./loom-connection.js";
import { runTool } from "./tool-result.js";

export const runLoomTool = <Details, E>(
  context: Pick<ExtensionContext, "cwd" | "sessionManager">,
  options: { readonly signal?: AbortSignal },
  requestTimeout: Duration.Input,
  operation: (
    client: LoomClientShape,
    sessionId: SessionId,
  ) => Effect.Effect<AgentToolResult<Details>, E | DaemonUnavailableError>,
): Promise<AgentToolResult<Details>> =>
  runTool(
    runWithLoomClient(context.cwd, requestTimeout, (client) =>
      operation(client, SessionId.make(context.sessionManager.getSessionId())),
    ),
    options,
  );
