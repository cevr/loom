# Job Pipeline Prototype

This throwaway daemon prototype answers one question.

Can a daemon-owned Effect child process return a Job handle after a Foreground Lease, survive caller cancellation, keep complete output, and stop its full process group with bounded escalation?

Run it with one command:

```sh
bun run prototype:job-pipeline
```

The prototype uses a short lease and a short kill grace period. This keeps the run quick. Production policy uses a five-minute default lease and a five-second kill grace period.

The command starts a shell pipeline that ignores `SIGTERM`. The pipeline keeps both ends active. This forces the Effect child-process adapter to use `SIGKILL` after the grace period.

The program prints each observed state. It exits with a failure when an expected contract does not hold.

This code is not production code. Remove it after the decision is captured.
