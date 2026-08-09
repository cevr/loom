import { Schema } from "effect";

export class HerdrClientError extends Schema.TaggedError<HerdrClientError>()("HerdrClientError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}
