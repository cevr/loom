import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export type LoomExtensionApi = Pick<
  ExtensionAPI,
  "on" | "registerCommand" | "registerTool" | "sendMessage"
>;
