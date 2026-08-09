# Supervise Code Kernels as Bun processes

Loom runs each Code Kernel in a separate Bun process.
The Loom daemon owns the process scope.
The daemon uses Effect Child Process to start and stop the worker.

The daemon and worker use a typed JSONL protocol.
Request IDs correlate each request and response.
The worker processes requests in serial order.

The daemon replaces the worker after a timeout, process exit, or protocol failure.
The daemon keeps the worker after a compilation or execution error.
The daemon never replays Cells into a replacement worker.

This boundary protects daemon availability from a blocked or failed Code Kernel.
It does not provide a security sandbox.
