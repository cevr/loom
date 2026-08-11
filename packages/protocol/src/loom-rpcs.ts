import { CoreRpcs } from "./core-rpcs.js";
import { JobRpcs } from "./job-rpcs.js";
import { PluginStateRpcs } from "./plugin-state-rpcs.js";
import { WorkflowRpcs } from "./workflow-rpcs.js";

export class LoomRpcs extends CoreRpcs.merge(JobRpcs, WorkflowRpcs, PluginStateRpcs) {}
