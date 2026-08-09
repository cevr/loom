/* oxlint-disable effect/noAsyncFunction, effect/noGlobals -- This worker fixture must fail outside Effect. */
await Bun.stderr.write(`head:${"x".repeat(512)}:tail\n`);
process.exit(23);
