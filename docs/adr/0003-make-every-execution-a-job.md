# Make every execution a Job

Every execution starts as a Job. A foreground request only leases the Job for a bounded wait and receives a Job ID when the lease ends. This design lets long pipelines continue without blocking the agent and keeps cancellation, output, and recovery under one ownership model.
