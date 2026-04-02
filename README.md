# Concerto Bus Benchmark

Kubernetes-native benchmark platform for Apache Kafka, RabbitMQ, ActiveMQ Artemis, and NATS JetStream.

## Start

```bash
bash scripts/bootstrap-python-env.sh
bash scripts/start-platform.sh
```

The start script:

- wipes the previous runtime release and run namespaces
- preserves PostgreSQL, MinIO, and stored runs by default
- installs missing broker operators
- installs platform-data operators and services only when external storage is requested
- rebuilds and redeploys the runtime
- prints the full access address when the deploy completes

`bootstrap-python-env.sh` creates the shared repo virtual environment and installs the runtime dependencies needed by the operational scripts. Add `--dev` when you also want test and Playwright dependencies.

For an in-place rollout without deleting stored runs first:

```bash
bash scripts/start-platform.sh --no-clean
```

To wipe external platform data on purpose:

```bash
bash scripts/start-platform.sh --wipe-data
```

## Prerequisites

- `kubectl`
- `helm`
- `docker`
- `curl`
- `bash`
- `python3` or `python`
- access to a Kubernetes cluster

If the cluster cannot consume locally built images, publish the images first or set a reachable registry tag through `--repository` and `--tag`.

## Access

Default deploy path:

```bash
bash scripts/deploy-platform.sh
```

That script picks a stable local-access strategy and prints the final URL:

- local clusters (`k3s`, `k3d`, `minikube`, `kind`, Docker Desktop, MicroK8s): `NodePort` by default
  - native local clusters publish `127.0.0.1:30080`
  - WSL-backed `k3s` publishes the reachable WSL node IP instead of a fake localhost URL
- remote clusters: ingress
- explicit `--ingress-host` always wins for ingress deployments

Examples:

```bash
bash scripts/deploy-platform.sh --access-mode nodeport
bash scripts/deploy-platform.sh --ingress-host bench.example.com
```

The portability contract is now:

- local clusters do not depend on guessed `sslip.io` hosts
- WSL-backed local clusters publish the reachable node IP for `NodePort` access
- remote ingress hosts are explicit or discovered from a real ingress frontend
- the deploy script fails fast instead of printing a fake URL

## Main flow

1. Open the UI.
2. In `Benchmark`, choose the broker, setup, and test window.
3. Start one run immediately or queue it in sequential mode.
4. The platform creates a dedicated run namespace, deploys the selected broker runtime, starts producer and consumer jobs, and stores measured results.
5. A detached finalizer job aggregates stored artifacts after the run window completes.
6. In `Results`, review completed runs.
7. In `Reports`, review the report preview and export a PDF from completed runs.

Only one run executes at a time. Parallel mode requires an open slot. Sequential mode keeps later runs in `queued` and `waiting` states until the active benchmark completes. When a run finishes or is stopped, the run namespace is deleted. Operators stay installed.

## Benchmark controls

The Benchmark tab now supports:

- sequential scheduling
- message marker ranges with optional wraparound
- broker-safe payload limit policy
- fresh payload generation or deliberate payload template reuse

Large payloads are validated against broker-safe limits after CloudEvents overhead is considered. In `auto adjust` mode the platform expands broker-safe limits when it can. In `strict reject` mode it fails fast instead.

## Scenario catalog

The scenario catalog is now organized into professional templates instead of ad hoc presets:

- `Low latency`: interactive and tail-latency focused profiles
- `High throughput`: concurrency and partition pressure profiles
- `Fanout`: one-to-many delivery profiles
- `Large payload`: buffer, batching, and I/O heavy profiles
- `Durable delivery`: stronger persistence and HA-oriented profiles

Each scenario template carries broker-specific defaults for:

- setup preset and deployment mode
- producer and consumer counts
- payload size and load shape
- broker/client tuning overrides
- CPU, memory, storage, storage class, and replica recommendations

## Deployment layout

- `deploy/charts/bus-platform`: Helm chart for the runtime
- `deploy/kustomize/cluster/portable`: cluster bootstrap overlay
- `deploy/k8s/brokers`: broker manifests and compatibility fallbacks
- `deploy/k8s/operators/versions.yaml`: pinned broker, operator, and runtime chart versions

Helm is used for the runtime lifecycle. Kustomize is used for cluster bootstrap and future overlay-friendly cluster customization.
The runtime uses Helm. Cluster bootstrap stays in Kustomize. Control planes are intentionally mixed by upstream packaging model:

- Strimzi: Helm chart
- RabbitMQ Cluster Operator: upstream manifest
- Artemis operator: Helm chart
- NACK: Helm chart
- CloudNativePG: Helm chart
- MinIO Operator and tenant: Helm charts

NATS uses a real installed control plane too: a cluster-scoped `nats/nack` controller is installed in `bench-nats-operator`, while each run still gets its own isolated `nats/nats` runtime chart.

The runtime chart now binds a purpose-built cluster role by default. `cluster-admin` remains only as an explicit fallback chart option.

## Measurement contract

The primary metric is producer-to-consumer end-to-end latency for the same CloudEvent:

- producer send timestamp is written into the CloudEvent envelope
- consumer receive timestamp is recorded when the consumer receives that CloudEvent
- latency is computed from producer send to consumer receive
- raw producer records, raw consumer records, and resource samples are persisted for audit and report generation

CloudEvents standardizes the envelope and metadata. It does not add timing precision by itself.

## Reports

Reports are generated only from stored measured runs. They include:

- summary tables
- p99 latency comparison
- throughput comparison
- latency-over-time view
- latency distribution
- configuration appendix
- artifact counts and timing notes

The web `Reports` tab stays table-based. Charts are generated in the PDF itself from stored measured data.

Completed runs with degraded delivery or zero latency samples still appear in `Results` and `Reports`. They are marked with diagnostics instead of being hidden.

## Verify

```bash
bash scripts/test-platform.sh
bash scripts/verify-platform.sh
```

The verifier checks runtime health, the published access URL, broker operators, CNPG, MinIO, and absence of legacy fixed broker namespaces.
`scripts/test-platform.sh` creates a local virtual environment if needed, installs the Linux test dependencies, installs the Playwright Chromium browser unless `SKIP_PLAYWRIGHT_INSTALL=1`, and runs the test suite.

For explicit platform-data bootstrap without deploying the web runtime:

```bash
bash scripts/bootstrap-platform-data.sh
```

## Repo map

- `scripts`: Linux-first operational entrypoints
- `services/platform`: API, UI, report generation, cluster orchestration
- `services/benchmark-agent`: producer and consumer worker
- `catalog`: scenario defaults and broker profiles
- `deploy`: Helm chart, Kustomize overlays, and broker manifests
- `docs`: concise product and architecture notes

## Runtime data

Default runtime mode is local storage:

- SQLite in `runtime/bus.db`
- repo-local artifacts in `runtime/artifacts`
- repo-local reports in `runtime/reports`

External storage is explicit:

- PostgreSQL via CloudNativePG in `bench-platform-data`
- S3-compatible artifact storage via MinIO in `bench-platform-data`

Use `--storage-mode external` when you want the full cluster-backed data plane. The deploy script will bootstrap the required platform-data operators and services before the runtime rollout.

## Images

Custom images are built under Docker Hub namespace `ninefinger9`.

Only two images are project-specific:

- `ninefinger9/bus-platform`
- `ninefinger9/bus-benchmark-agent`

Everything else comes from upstream charts or manifests at deploy time:

- Kafka and Strimzi operator images
- RabbitMQ operator and broker images
- ArtemisCloud operator and broker images
- NATS and NACK images

For portability:

- local `k3s` can import the two custom images directly without pushing them
- other clusters can use any reachable registry by overriding `REPOSITORY` or Helm `images.*.repository`
- the chart defaults point at `ninefinger9/*`, but that is only a default, not a hard runtime requirement
