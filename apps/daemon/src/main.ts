import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Effect } from "effect";
import { program } from "./program.js";

BunRuntime.runMain(program.pipe(Effect.provide(BunServices.layer)));
