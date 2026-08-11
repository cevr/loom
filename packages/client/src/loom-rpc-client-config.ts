import type { WorkspaceRoot } from "@cvr/loom-domain";
import type { Duration } from "effect";

export interface LoomRpcClientConfig {
  readonly socketPath: string;
  readonly workspaceRoot: WorkspaceRoot;
  readonly connectionTimeout?: Duration.Input;
  readonly requestTimeout?: Duration.Input;
}
