import type { LoomExtensionApi } from "./extension-api.js";

export const loomThemeName = "loom-rose-pine";

export const registerLoomTheme = (pi: LoomExtensionApi): void => {
  pi.on("session_start", (_event, context) => {
    const selected = context.ui.setTheme(loomThemeName);
    if (selected.success) return;
    context.ui.notify(selected.error ?? "Loom Rosé Pine theme is unavailable.", "error");
  });
};
