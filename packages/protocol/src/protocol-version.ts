import { Schema } from "effect";

export const ProtocolVersion = Schema.Int.check(Schema.isGreaterThan(0));
export type ProtocolVersion = typeof ProtocolVersion.Type;

export const currentProtocolVersion = 7;
export const minimumProtocolVersion = currentProtocolVersion;
export const maximumProtocolVersion = currentProtocolVersion;
export const maximumFrameSize = 1024 * 1024;
export const maximumCellSourceLength = maximumFrameSize - 4096;
export const maximumCellDisplayLength = 64 * 1024;
export const maximumCellBindings = 1024;
