# Product Story

The platform is one workspace for benchmarking messaging brokers.

## User path

1. Start the platform with `scripts/start-platform.sh`.
2. Open the UI.
3. In `Benchmark`, choose one broker, one setup, and one test window.
4. Start one run.
5. The platform creates a dedicated broker namespace, starts benchmark jobs, and stores measurement material durably.
6. A detached finalizer job turns stored artifacts into the completed run metrics.
7. The platform deletes the run topology when the run finishes or is stopped.
8. The user reviews completed runs in `Results`.
9. The user builds a PDF in `Reports` from stored measured runs.

## Product rules

- one active run at a time
- one Linux-first deployment path
- no placeholder metrics in normal execution
- no report built from fabricated data
- no completed or stopped run leaves broker topology behind

## Core contract

The primary metric is end-to-end latency from producer send to consumer receive for the same CloudEvent.
