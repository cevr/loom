import type { LoomExtensionApi } from "./extension-api.js";
import { registerGoal } from "./goal.js";
import { registerSideConversation } from "./side-conversation.js";

export const registerClientComponents = (pi: LoomExtensionApi): void => {
  registerSideConversation(pi);
  registerGoal(pi);
};
