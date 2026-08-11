/* oxlint-disable effect/noAsyncFunction, effect/noGlobals, no-await-in-loop -- Bun process fixture. */
const prompt = await Bun.stdin.text();
const waitPrefix = "wait-for:";

if (prompt.startsWith(waitPrefix)) {
  const path = prompt.slice(waitPrefix.length).trim();
  while (!(await Bun.file(path).exists())) await Bun.sleep(10);
}

await Bun.stdout.write(`agent-complete:${prompt.trim()}\n`);
