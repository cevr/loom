import { runCodeKernelWorker } from "@cvr/loom-platform-bun";
import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Effect } from "effect";

BunRuntime.runMain(runCodeKernelWorker.pipe(Effect.provide(BunServices.layer)));
