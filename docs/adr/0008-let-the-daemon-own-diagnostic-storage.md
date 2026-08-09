# Let the daemon own diagnostic storage

The Code Kernel factory owns one diagnostic store.
Direct Code Kernel construction keeps stderr in memory only.

The store uses one semaphore for file allocation and cleanup.
It reserves each file before stderr capture starts.
The worker scope releases the reservation after stderr capture stops.
An active file cannot be removed.
The store rejects a new file when active files fill a limit.
The worker still runs without that file.

The store runs cleanup when the daemon starts.
It also runs cleanup before each file allocation.
It removes the oldest inactive file first.
It removes empty Agent and Session directories.
Cleanup failure does not stop the daemon or the worker.

The store resolves the diagnostics root to its canonical path.
It walks only the Session and Agent directory levels.
It ignores an owner directory that resolves outside the root.
It removes only Loom stderr file names.

This store gives one daemon a global storage limit.
It does not coordinate multiple daemons that use the same root.
