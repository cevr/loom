import { Schema } from "effect";

export const ProtocolVersion = Schema.Int.check(Schema.isGreaterThan(0));
export type ProtocolVersion = typeof ProtocolVersion.Type;

export const currentProtocolVersion = 1;
export const minimumProtocolVersion = 1;
export const maximumProtocolVersion = currentProtocolVersion;
export const maximumFrameSize = 1024 * 1024;
