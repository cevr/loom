# Separate Code Kernels from Workflow Runs

A Code Kernel owns mutable interactive state. A Workflow Run owns durable replay state. Loom keeps these models separate because replaying arbitrary interactive cells can repeat file, network, and process effects.
