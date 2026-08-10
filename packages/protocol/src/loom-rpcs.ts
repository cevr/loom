import { CoreRpcs } from "./core-rpcs.js";
import { JobRpcs } from "./job-rpcs.js";
import { WorkflowRpcs } from "./workflow-rpcs.js";

export class LoomRpcs extends CoreRpcs.merge(JobRpcs, WorkflowRpcs) {}
