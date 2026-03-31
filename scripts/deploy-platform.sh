#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

REPOSITORY="${REPOSITORY:-ninefinger9}"
TAG="${TAG:-}"
BUILD_ID="${BUILD_ID:-}"
ASSET_VERSION="${ASSET_VERSION:-}"
SKIP_OPERATOR_INSTALL=0
IMAGE_PULL_SECRET="${IMAGE_PULL_SECRET:-}"
PLATFORM_PULL_POLICY="IfNotPresent"
STORAGE_MODE="${STORAGE_MODE:-local}"
ACCESS_MODE="${ACCESS_MODE:-auto}"
LOCAL_NODE_PORT="${LOCAL_NODE_PORT:-30080}"
INGRESS_HOST="${INGRESS_HOST:-}"
INGRESS_CLASS="${INGRESS_CLASS:-}"
INGRESS_HOST_MODE="${INGRESS_HOST_MODE:-auto}"
INGRESS_BOOTSTRAP_HOST="${INGRESS_BOOTSTRAP_HOST:-bus-platform-pending.invalid}"
INGRESS_AUTO_DNS_SUFFIX="${INGRESS_AUTO_DNS_SUFFIX:-sslip.io}"
INGRESS_CLUSTER_SLUG="${INGRESS_CLUSTER_SLUG:-}"
BOOTSTRAP_PLATFORM_DATA="${BOOTSTRAP_PLATFORM_DATA:-0}"
LOCAL_ACCESS_HOST="${LOCAL_ACCESS_HOST:-}"

default_deploy_tag() {
  local stamp
  stamp="$(date -u +%Y%m%d%H%M%S)"
  if git -C "${REPO_ROOT}" rev-parse --short HEAD >/dev/null 2>&1; then
    printf '%s-%s\n' "${stamp}" "$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
    return 0
  fi
  printf '%s\n' "${stamp}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repository)
      REPOSITORY="$2"
      shift 2
      ;;
    --tag)
      TAG="$2"
      shift 2
      ;;
    --skip-operator-install)
      SKIP_OPERATOR_INSTALL=1
      shift
      ;;
    --storage-mode)
      STORAGE_MODE="$2"
      shift 2
      ;;
    --access-mode)
      ACCESS_MODE="$2"
      shift 2
      ;;
    --local-node-port)
      LOCAL_NODE_PORT="$2"
      shift 2
      ;;
    --bootstrap-platform-data)
      BOOTSTRAP_PLATFORM_DATA=1
      shift
      ;;
    --ingress-host)
      INGRESS_HOST="$2"
      shift 2
      ;;
    --ingress-class)
      INGRESS_CLASS="$2"
      shift 2
      ;;
    --ingress-host-mode)
      INGRESS_HOST_MODE="$2"
      shift 2
      ;;
    *)
      printf 'Unknown argument: %s\n' "$1" >&2
      exit 1
      ;;
  esac
done

if [[ -z "${TAG}" ]]; then
  TAG="$(default_deploy_tag)"
fi
if [[ -z "${BUILD_ID}" ]]; then
  BUILD_ID="${TAG}"
fi
if [[ -z "${ASSET_VERSION}" ]]; then
  ASSET_VERSION="${BUILD_ID}"
fi

require_cmd kubectl
require_cmd docker
require_cmd curl
resolve_python_cmd >/dev/null

detect_ingress_class() {
  local class_name
  if [[ -n "${INGRESS_CLASS}" ]]; then
    printf '%s\n' "${INGRESS_CLASS}"
    return 0
  fi
  class_name="$(
    kubectl get ingressclass \
      -o jsonpath='{range .items[?(@.metadata.annotations.ingressclass\.kubernetes\.io/is-default-class=="true")]}{.metadata.name}{"\n"}{end}' \
      2>/dev/null | awk 'NF { print; exit }'
  )"
  if [[ -n "${class_name}" ]]; then
    printf '%s\n' "${class_name}"
    return 0
  fi
  for candidate in traefik nginx ingress-nginx; do
    if kubectl get ingressclass "${candidate}" >/dev/null 2>&1; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  printf '\n'
}

sanitize_dns_label() {
  local value="$1"
  value="$(printf '%s' "${value}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//; s/-{2,}/-/g')"
  value="${value:0:40}"
  printf '%s\n' "${value:-cluster}"
}

detect_cluster_slug() {
  if [[ -n "${INGRESS_CLUSTER_SLUG}" ]]; then
    sanitize_dns_label "${INGRESS_CLUSTER_SLUG}"
    return 0
  fi
  local current_context
  current_context="$(kubectl config current-context 2>/dev/null || true)"
  sanitize_dns_label "${current_context:-cluster}"
}

detect_local_cluster() {
  local current_context
  current_context="$(kubectl config current-context 2>/dev/null || true)"
  if command -v k3s >/dev/null 2>&1; then
    return 0
  fi
  if [[ "${current_context}" =~ (k3s|k3d|minikube|docker-desktop|kind|microk8s) ]]; then
    return 0
  fi
  return 1
}

detect_wsl_environment() {
  if [[ -n "${WSL_DISTRO_NAME:-}" ]]; then
    return 0
  fi
  if grep -qi microsoft /proc/version 2>/dev/null; then
    return 0
  fi
  return 1
}

is_private_ipv4() {
  local ip="$1"
  [[ "${ip}" =~ ^10\. ]] && return 0
  [[ "${ip}" =~ ^192\.168\. ]] && return 0
  [[ "${ip}" =~ ^172\.(1[6-9]|2[0-9]|3[0-1])\. ]] && return 0
  [[ "${ip}" =~ ^127\. ]] && return 0
  return 1
}

detect_ingress_hostname() {
  kubectl -n bench-platform get ingress bus-platform -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || true
}

detect_ingress_ip() {
  kubectl -n bench-platform get ingress bus-platform -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || true
}

detect_node_ip() {
  kubectl get nodes \
    -o jsonpath='{range .items[*]}{range .status.addresses[?(@.type=="ExternalIP")]}{.address}{"\n"}{end}{range .status.addresses[?(@.type=="InternalIP")]}{.address}{"\n"}{end}{end}' \
    2>/dev/null | awk 'NF { print; exit }'
}

detect_nodeport_access_host() {
  if [[ -n "${LOCAL_ACCESS_HOST}" ]]; then
    printf '%s\n' "${LOCAL_ACCESS_HOST}"
    return 0
  fi
  if detect_wsl_environment; then
    local node_ip=""
    node_ip="$(detect_node_ip)"
    if [[ -n "${node_ip}" ]]; then
      printf '%s\n' "${node_ip}"
      return 0
    fi
    hostname -I 2>/dev/null | awk 'NF { print $1; exit }'
    return 0
  fi
  printf '127.0.0.1\n'
}

wait_for_ingress_frontend() {
  local attempts=0
  local max_attempts=45
  local detected=""
  while (( attempts < max_attempts )); do
    detected="$(detect_ingress_hostname)"
    if [[ -n "${detected}" ]]; then
      printf '%s\n' "${detected}"
      return 0
    fi
    detected="$(detect_ingress_ip)"
    if [[ -n "${detected}" ]]; then
      if [[ "${detected}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] && is_private_ipv4 "${detected}"; then
        detected=""
      else
        printf '%s\n' "${detected}"
        return 0
      fi
    fi
    detected="$(detect_node_ip)"
    if [[ -n "${detected}" ]]; then
      if [[ "${detected}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] && ! is_private_ipv4 "${detected}"; then
        printf '%s\n' "${detected}"
        return 0
      fi
    fi
    attempts=$((attempts + 1))
    sleep 4
  done
  return 1
}

determine_access_mode() {
  local requested
  requested="$(printf '%s' "${ACCESS_MODE}" | tr '[:upper:]' '[:lower:]')"
  if [[ "${requested}" == "auto" ]]; then
    if detect_local_cluster; then
      printf 'nodeport\n'
      return 0
    fi
    printf 'ingress\n'
    return 0
  fi
  if [[ "${requested}" =~ ^(nodeport|ingress)$ ]]; then
    printf '%s\n' "${requested}"
    return 0
  fi
  printf 'Unsupported access mode: %s\n' "${ACCESS_MODE}" >&2
  exit 1
}

wait_for_url() {
  local url="$1"
  local timeout_seconds="${2:-120}"
  local deadline=$((SECONDS + timeout_seconds))
  until curl -fsS "${url}" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      return 1
    fi
    sleep 2
  done
  return 0
}

build_auto_host() {
  local frontend="$1"
  local cluster_slug="$2"
  if [[ "${frontend}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    local dashed_ip="${frontend//./-}"
    printf 'bus-platform.bench-platform.%s.%s.%s\n' "${cluster_slug}" "${dashed_ip}" "${INGRESS_AUTO_DNS_SUFFIX}"
    return 0
  fi
  printf '%s\n' "${frontend}"
}

run_helm_upgrade() {
  local access_mode="$1"
  local service_type="$2"
  local access_url="$3"
  local host_mode="$4"
  local host_value="$5"
  local class_name="$6"
  local cluster_slug="$7"
  local platform_image="${REPOSITORY}/bus-platform:${TAG}"
  local agent_image="${REPOSITORY}/bus-benchmark-agent:${TAG}"
  local chart_path="${REPO_ROOT}/deploy/charts/bus-platform"

  local ingress_enabled="true"
  if [[ "${access_mode}" == "nodeport" ]]; then
    ingress_enabled="false"
  fi

  local -a helm_args=(
    upgrade --install bus-platform "${chart_path}"
    --namespace bench-platform
    --create-namespace
    --take-ownership
    --wait
    --timeout 300s
    --set "images.platform.repository=${REPOSITORY}/bus-platform"
    --set "images.platform.tag=${TAG}"
    --set "images.platform.pullPolicy=${PLATFORM_PULL_POLICY}"
    --set "images.agent.repository=${REPOSITORY}/bus-benchmark-agent"
    --set "images.agent.tag=${TAG}"
    --set "env.benchmarkAgentImagePullPolicy=${PLATFORM_PULL_POLICY}"
    --set-string "env.buildId=${BUILD_ID}"
    --set-string "env.assetVersion=${ASSET_VERSION}"
    --set-string "env.storageMode=${STORAGE_MODE}"
    --set-string "env.accessUrl=${access_url}"
    --set-string "agentImagePullSecret=${IMAGE_PULL_SECRET}"
    --set "service.type=${service_type}"
    --set "service.nodePort=${LOCAL_NODE_PORT}"
    --set-string "service.localAccessHost=${LOCAL_ACCESS_HOST}"
    --set "ingress.enabled=${ingress_enabled}"
    --set "ingress.hostMode=${host_mode}"
    --set "ingress.bootstrapHost=${INGRESS_BOOTSTRAP_HOST}"
    --set "ingress.autoDnsSuffix=${INGRESS_AUTO_DNS_SUFFIX}"
    --set "ingress.clusterSlug=${cluster_slug}"
  )
  if [[ -n "${host_value}" ]]; then
    helm_args+=(--set "ingress.host=${host_value}")
  fi
  if [[ -n "${class_name}" ]]; then
    helm_args+=(--set "ingress.className=${class_name}")
  fi
  if [[ -n "${IMAGE_PULL_SECRET}" ]]; then
    helm_args+=(--set "imagePullSecrets[0]=${IMAGE_PULL_SECRET}")
  fi
  helm "${helm_args[@]}"
}

if [[ "${SKIP_OPERATOR_INSTALL}" != "1" ]]; then
  "${SCRIPT_DIR}/install-broker-operators.sh"
fi

if [[ "${STORAGE_MODE}" == "external" || "${BOOTSTRAP_PLATFORM_DATA}" == "1" ]]; then
  "${SCRIPT_DIR}/bootstrap-platform-data.sh"
fi

platform_image="${REPOSITORY}/bus-platform:${TAG}"
agent_image="${REPOSITORY}/bus-benchmark-agent:${TAG}"

log "Using build id ${BUILD_ID} and asset version ${ASSET_VERSION}"
log "Building platform image ${platform_image}"
docker build -f "${REPO_ROOT}/services/platform/Dockerfile" -t "${platform_image}" "${REPO_ROOT}"

log "Building benchmark agent image ${agent_image}"
docker build -f "${REPO_ROOT}/services/benchmark-agent/Dockerfile" -t "${agent_image}" "${REPO_ROOT}"

if ! import_local_image_if_supported "${platform_image}"; then
  log "No local cluster import hook detected. Pushing ${platform_image} to the registry."
  docker push "${platform_image}"
  PLATFORM_PULL_POLICY="Always"
fi

if ! import_local_image_if_supported "${agent_image}"; then
  log "No local cluster import hook detected. Pushing ${agent_image} to the registry."
  docker push "${agent_image}"
fi

log "Deploying platform runtime"
kubectl apply -k "${REPO_ROOT}/deploy/kustomize/cluster/portable"
if ! helm -n bench-platform status bus-platform >/dev/null 2>&1; then
  log "Cleaning pre-Helm runtime resources before Helm install"
  kubectl -n bench-platform delete deployment/bus-platform service/bus-platform ingress/bus-platform serviceaccount/bus-platform-control --ignore-not-found=true >/dev/null 2>&1 || true
  kubectl delete clusterrolebinding bus-platform-control --ignore-not-found=true >/dev/null 2>&1 || true
fi
kubectl delete clusterrolebinding bus-platform-control --ignore-not-found=true >/dev/null 2>&1 || true
kubectl -n bench-platform delete deployment/bus-platform --ignore-not-found=true --wait=true >/dev/null 2>&1 || true

resolved_ingress_class="$(detect_ingress_class)"
cluster_slug="$(detect_cluster_slug)"
resolved_access_mode="$(determine_access_mode)"
effective_host_mode="$(printf '%s' "${INGRESS_HOST_MODE}" | tr '[:upper:]' '[:lower:]')"
resolved_access_url=""
verification_base_url=""

if [[ -n "${INGRESS_HOST}" ]]; then
  effective_host_mode="manual"
fi

if [[ "${resolved_access_mode}" == "nodeport" ]]; then
  LOCAL_ACCESS_HOST="$(detect_nodeport_access_host)"
  resolved_access_url="http://${LOCAL_ACCESS_HOST}:${LOCAL_NODE_PORT}/"
  verification_base_url="${resolved_access_url}"
  if detect_wsl_environment && [[ "${LOCAL_ACCESS_HOST}" != "127.0.0.1" ]]; then
    verification_base_url="http://127.0.0.1:${LOCAL_NODE_PORT}/"
  fi
  log "Deploying with local NodePort access at ${resolved_access_url}"
  run_helm_upgrade "nodeport" "NodePort" "${resolved_access_url}" "hostless" "" "${resolved_ingress_class}" "${cluster_slug}"
  kubectl -n bench-platform rollout status deployment bus-platform --timeout=300s
  delete_legacy_broker_namespaces
  BASE_URL="${verification_base_url}" EXPECTED_STORAGE_MODE="${STORAGE_MODE}" "${SCRIPT_DIR}/verify-platform.sh"
  log "Deployment complete."
  log "Concerto Bus Benchmark address: ${resolved_access_url}"
  exit 0
fi

if [[ "${effective_host_mode}" == "hostless" ]]; then
  printf 'Hostless ingress is not a supported published access mode for deploy-platform.sh. Use --access-mode nodeport on local clusters or set --ingress-host for ingress deployments.\n' >&2
  exit 1
fi

if [[ -n "${INGRESS_HOST}" ]]; then
  resolved_access_url="http://${INGRESS_HOST}/"
  log "Deploying with explicit ingress host ${INGRESS_HOST}"
  run_helm_upgrade "ingress" "ClusterIP" "${resolved_access_url}" "manual" "${INGRESS_HOST}" "${resolved_ingress_class}" "${cluster_slug}"
else
  log "Deploying with bootstrap ingress host ${INGRESS_BOOTSTRAP_HOST}"
  run_helm_upgrade "ingress" "ClusterIP" "" "auto" "${INGRESS_BOOTSTRAP_HOST}" "${resolved_ingress_class}" "${cluster_slug}"
  if frontend_address="$(wait_for_ingress_frontend)"; then
    auto_host="$(build_auto_host "${frontend_address}" "${cluster_slug}")"
    if [[ -n "${auto_host}" && "${auto_host}" != "${INGRESS_BOOTSTRAP_HOST}" ]]; then
      log "Resolved ingress host ${auto_host} from cluster frontend ${frontend_address}"
      resolved_access_url="http://${auto_host}/"
      run_helm_upgrade "ingress" "ClusterIP" "${resolved_access_url}" "auto" "${auto_host}" "${resolved_ingress_class}" "${cluster_slug}"
      INGRESS_HOST="${auto_host}"
    fi
  else
    printf 'Ingress frontend could not be resolved automatically. Set --ingress-host explicitly or use --access-mode nodeport for local clusters.\n' >&2
    exit 1
  fi
fi

kubectl -n bench-platform rollout status deployment bus-platform --timeout=300s
delete_legacy_broker_namespaces
if [[ -n "${resolved_access_url}" ]]; then
  wait_for_url "${resolved_access_url%/}/api/health" 180
fi
BASE_URL="${resolved_access_url}" EXPECTED_STORAGE_MODE="${STORAGE_MODE}" "${SCRIPT_DIR}/verify-platform.sh"

log "Deployment complete."
log "Concerto Bus Benchmark address: ${resolved_access_url}"
