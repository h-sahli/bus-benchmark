#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

BROKER=""
MODE="baseline"
DEPLOYMENT_MODE="normal"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --broker)
      BROKER="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --deployment-mode)
      DEPLOYMENT_MODE="$2"
      shift 2
      ;;
    *)
      printf 'Unknown argument: %s\n' "$1" >&2
      exit 1
      ;;
  esac
done

case "${BROKER}" in
  kafka|rabbitmq|artemis) ;;
  *)
    printf 'Missing or unsupported --broker. Use kafka, rabbitmq, or artemis.\n' >&2
    exit 1
    ;;
esac

case "${MODE}" in
  baseline|optimized) ;;
  *)
    printf 'Unsupported --mode. Use baseline or optimized.\n' >&2
    exit 1
    ;;
esac

case "${DEPLOYMENT_MODE}" in
  normal|ha) ;;
  *)
    printf 'Unsupported --deployment-mode. Use normal or ha.\n' >&2
    exit 1
    ;;
esac

manifest_name="${BROKER}-${MODE}.yaml"
if [[ "${DEPLOYMENT_MODE}" == "ha" ]]; then
  manifest_name="${BROKER}-${MODE}-ha.yaml"
fi
manifest_path="${REPO_ROOT}/deploy/k8s/brokers/${manifest_name}"

if [[ ! -f "${manifest_path}" ]]; then
  printf 'Broker manifest not found: %s\n' "${manifest_path}" >&2
  exit 1
fi

log "Applying ${BROKER} ${MODE} profile in ${DEPLOYMENT_MODE} mode from ${manifest_path}"
kubectl apply -f "${manifest_path}"

target_replicas=1
if [[ "${DEPLOYMENT_MODE}" == "ha" ]]; then
  target_replicas=3
fi

case "${BROKER}" in
  kafka)
    kubectl -n bench-kafka wait "kafka/${BROKER}-${MODE}" --for=condition=Ready --timeout=900s
    ;;
  rabbitmq)
    kubectl -n bench-rabbitmq wait "rabbitmqcluster/${BROKER}-${MODE}" --for=condition=AllReplicasReady --timeout=900s
    ;;
  artemis)
    kubectl -n bench-artemis wait "activemqartemis/${BROKER}-${MODE}" --for=condition=Valid --timeout=900s
    kubectl -n bench-artemis wait --for=condition=Ready pod --all --timeout=900s
    kubectl -n bench-artemis get secret "${BROKER}-${MODE}-credentials-secret" -o name >/dev/null
    ;;
esac

log "${BROKER} profile is ready in ${DEPLOYMENT_MODE} mode with target replicas=${target_replicas}."
