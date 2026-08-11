# Assign one owner to each orchestration fact

Loom stores each durable coordination fact once. The Loom Orchestration Store owns local requests, identities, retention deadlines, and ownership records. Effect Cluster storage owns persisted mailboxes and Workflow execution facts. Files own complete logs and large Artifacts. Session and normal Agent identities remain namespaces. One Session closure row owns the temporary late-work fence and its fixed deadline. Derived projections do not become recovery authority. This split prevents conflicting recovery decisions after daemon restart.
