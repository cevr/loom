/* oxlint-disable effect/noAsyncFunction, effect/noGlobals -- This worker fixture must fail outside Effect. */
await Bun.stderr.write("loom kernel boot failure\n");
process.exit(23);
