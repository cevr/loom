# Use capability-based Plugin Components

A Loom Plugin can contain one Component for each supported host. Each Component declares its Plugin Grants and Plugin Contributions. The host validates the complete declaration before it builds the Component as an Effect Layer in a private Scope. Loom does not give a Plugin the daemon Context, raw stores, or a generic hook interface. This design keeps client actions separate from daemon authority and lets each host supervise Plugin work with OTP-style isolation.
