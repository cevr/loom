import { Schema } from "effect";

export class BunJsonlError extends Schema.TaggedError<BunJsonlError>()("BunJsonlError", {
  cause: Schema.Defect(),
}) {}
