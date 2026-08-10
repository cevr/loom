import { RpcGroup } from "effect/unstable/rpc";
import {
  CancelJob,
  DetachJob,
  InspectJob,
  ReadJobOutput,
  StartJob,
  WaitForJob,
} from "./job-control.js";

export class JobRpcs extends RpcGroup.make(
  StartJob,
  InspectJob,
  ReadJobOutput,
  WaitForJob,
  CancelJob,
  DetachJob,
) {}
