# Broker Manifests

Profiles are explicit and separated by broker and mode.

- `kafka-baseline.yaml`
- `kafka-optimized.yaml`
- `kafka-baseline-ha.yaml`
- `kafka-optimized-ha.yaml`
- `rabbitmq-baseline.yaml`
- `rabbitmq-optimized.yaml`
- `rabbitmq-baseline-ha.yaml`
- `rabbitmq-optimized-ha.yaml`
- `artemis-baseline.yaml`
- `artemis-optimized.yaml`
- `artemis-baseline-ha.yaml`
- `artemis-optimized-ha.yaml`

Use:

```bash
bash scripts/deploy-broker-profile.sh --broker kafka --mode baseline --deployment-mode normal
```

`baseline` profiles are configured as vendor-default-like for single-node fair comparison (minimal correctness overrides only).

Deployment mode options:

- `normal`: 1 replica
- `ha`: 3 replicas
