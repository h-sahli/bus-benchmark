# Control Planes

Operator and runtime chart versions are pinned in:

- [versions.yaml](versions.yaml)

Install command:

```bash
bash scripts/install-broker-operators.sh
```

Installation mode:

- Strimzi: Helm chart (`strimzi-kafka-operator`)
- RabbitMQ Cluster Operator: official upstream manifest from the RabbitMQ release channel
- ActiveMQ Artemis Operator: Helm chart from `oci://quay.io/arkmq-org/helm-charts/arkmq-org-broker-operator`
- CloudNativePG: Helm chart (`cloudnative-pg`)
- MinIO Operator: Helm chart (`minio-operator`)

NATS note:

- NATS JetStream now uses a cluster-scoped `nats/nack` controller in `bench-nats-operator`
- each benchmark namespace still uses the official `nats/nats` Helm chart for the broker runtime itself
- Stream CRDs carry the target NATS server address, so the installed NACK controller can reconcile resources for the isolated per-run NATS clusters

Helm values used by the installer and runtime bootstrap live under:

- `deploy/k8s/operators/helm-values/strimzi.yaml`
- `deploy/k8s/operators/helm-values/artemis.yaml`
- `deploy/k8s/operators/helm-values/cnpg.yaml`
- `deploy/k8s/operators/helm-values/minio-operator.yaml`

RabbitMQ note:

- the platform now installs the upstream RabbitMQ operator manifest directly
- the operator runs in the upstream `rabbitmq-system` namespace
- the old `bench-rabbitmq-operator` namespace is treated as legacy during migration and removed by the installer
