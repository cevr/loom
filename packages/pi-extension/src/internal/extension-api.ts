import type {
  ExtensionAPI,
  ExtensionContext,
  SessionShutdownEvent,
  SessionStartEvent,
} from "@earendil-works/pi-coding-agent";

export interface LoomExtensionApi {
  readonly on: {
    (
      event: "session_start",
      handler: (event: SessionStartEvent, context: ExtensionContext) => Promise<void> | void,
    ): void;
    (
      event: "session_shutdown",
      handler: (event: SessionShutdownEvent, context: ExtensionContext) => Promise<void> | void,
    ): void;
  };
  readonly registerCommand: ExtensionAPI["registerCommand"];
  readonly registerTool: ExtensionAPI["registerTool"];
}
