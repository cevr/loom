# Use Effect Cluster as the runtime

Loom uses Effect Cluster entities, persisted messages, and Cluster Workflow Engine as its actor runtime. This gives Loom durable OTP-like behavior without a second mailbox, sharding, or workflow engine. The first release uses `SingleRunner`, but entity code must remain compatible with several runners.
