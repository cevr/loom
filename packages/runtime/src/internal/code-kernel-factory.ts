import type { AgentOwner } from "@cvr/loom-domain";
import { Context, type Effect, type Scope } from "effect";
import type { CodeKernelShape } from "./code-kernel.js";

export interface CodeKernelFactoryShape {
  readonly spawn: (owner: AgentOwner) => Effect.Effect<CodeKernelShape, never, Scope.Scope>;
}

export class CodeKernelFactory extends Context.Service<CodeKernelFactory, CodeKernelFactoryShape>()(
  "@cvr/loom-runtime/CodeKernelFactory",
) {}
