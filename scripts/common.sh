#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

log() {
  printf '%s\n' "$*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    printf 'Missing required command: %s\n' "$1" >&2
    exit 1
  fi
}

resolve_python_cmd() {
  if command -v python3 >/dev/null 2>&1; then
    printf 'python3\n'
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    printf 'python\n'
    return 0
  fi
  printf 'Missing required command: python3 or python\n' >&2
  exit 1
}

ensure_repo_python_env() {
  local profile="${1:-runtime}"
  local python_cmd
  local venv_dir
  local marker_path
  local requirements_hash
  local requirement_files=(
    "${REPO_ROOT}/services/platform/requirements.txt"
    "${REPO_ROOT}/services/benchmark-agent/requirements.txt"
  )

  if [[ "${profile}" == "dev" ]]; then
    requirement_files+=("${REPO_ROOT}/services/platform/requirements-dev.txt")
  fi

  python_cmd="$(resolve_python_cmd)"
  venv_dir="${VENV_DIR:-${REPO_ROOT}/.venv}"
  marker_path="${venv_dir}/.bus-${profile}-requirements.sha256"

  if [[ ! -x "${venv_dir}/bin/python" ]]; then
    log "Creating Python virtual environment in ${venv_dir}"
    "${python_cmd}" -m venv "${venv_dir}" || {
      printf 'Failed to create Python virtual environment in %s\n' "${venv_dir}" >&2
      exit 1
    }
  fi

  requirements_hash="$(
    {
      "${venv_dir}/bin/python" --version 2>&1
      for file in "${requirement_files[@]}"; do
        printf 'FILE:%s\n' "${file}"
        cat "${file}"
      done
    } | sha256sum | awk '{print $1}'
  )"

  if [[ -f "${marker_path}" ]] && [[ "$(cat "${marker_path}")" == "${requirements_hash}" ]]; then
    return 0
  fi

  log "Installing Python dependencies for ${profile} tooling"
  "${venv_dir}/bin/python" -m pip install --upgrade pip >/dev/null || {
    printf 'Failed to upgrade pip in %s\n' "${venv_dir}" >&2
    exit 1
  }
  "${venv_dir}/bin/python" -m pip install -r "${REPO_ROOT}/services/platform/requirements.txt" -r "${REPO_ROOT}/services/benchmark-agent/requirements.txt" >/dev/null || {
    printf 'Failed to install runtime Python dependencies. Run bash scripts/bootstrap-python-env.sh for details.\n' >&2
    exit 1
  }
  if [[ "${profile}" == "dev" ]]; then
    "${venv_dir}/bin/python" -m pip install -r "${REPO_ROOT}/services/platform/requirements-dev.txt" >/dev/null || {
      printf 'Failed to install development Python dependencies. Run bash scripts/bootstrap-python-env.sh --dev for details.\n' >&2
      exit 1
    }
  fi

  printf '%s' "${requirements_hash}" > "${marker_path}"
}

repo_python_cmd() {
  local profile="${1:-runtime}"
  local venv_dir="${VENV_DIR:-${REPO_ROOT}/.venv}"
  ensure_repo_python_env "${profile}"
  printf '%s\n' "${venv_dir}/bin/python"
}

yaml_value() {
  local file_path="$1"
  local section_name="$2"
  local key_name="$3"
  awk -v section="${section_name}" -v key="${key_name}" '
    /^[^[:space:]][^:]*:[[:space:]]*$/ {
      current=$1
      sub(/:$/, "", current)
      in_section=(current == section)
      next
    }
    in_section {
      pattern="^[[:space:]]*" key ":[[:space:]]*"
      if ($0 ~ pattern) {
        value=$0
        sub(pattern, "", value)
        print value
        exit
      }
    }
  ' "${file_path}"
}

delete_legacy_broker_namespaces() {
  for namespace in bench-kafka bench-rabbitmq bench-artemis bench-nats; do
    kubectl delete namespace "${namespace}" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
  done
}

list_namespaces_by_prefix() {
  local prefix="$1"
  kubectl get namespaces -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
    | awk -v target_prefix="${prefix}" 'index($0, target_prefix) == 1 { print }'
}

wait_for_namespace_absent() {
  local namespace="$1"
  local timeout_seconds="${2:-300}"
  local deadline=$((SECONDS + timeout_seconds))
  while kubectl get namespace "${namespace}" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      printf 'Timed out waiting for namespace %s to disappear\n' "${namespace}" >&2
      return 1
    fi
    sleep 2
  done
}

clear_local_runtime_state() {
  rm -f "${REPO_ROOT}"/runtime/*.db
  rm -f "${REPO_ROOT}"/runtime/*.yaml
  rm -f "${REPO_ROOT}"/runtime/*.png
  rm -rf "${REPO_ROOT}/runtime/artifacts"
  rm -rf "${REPO_ROOT}/runtime/reports"
  rm -rf "${REPO_ROOT}/runtime/debug"
  mkdir -p "${REPO_ROOT}/runtime/debug"
}

import_local_image_if_supported() {
  local image="$1"
  if command -v k3s >/dev/null 2>&1; then
    log "Importing ${image} into the local cluster image store"
    if [[ "$(id -u)" == "0" ]]; then
      docker save "${image}" | k3s ctr images import -
      return 0
    fi
    if sudo -n true >/dev/null 2>&1; then
      docker save "${image}" | sudo k3s ctr images import -
      return 0
    fi
    if [[ -n "${WSL_DISTRO_NAME:-}" ]] && [[ -x /mnt/c/Windows/System32/wsl.exe ]]; then
      local archive_path
      archive_path="$(mktemp "${TMPDIR:-/tmp}/bus-image-import.XXXXXX.tar")"
      docker save -o "${archive_path}" "${image}"
      if /mnt/c/Windows/System32/wsl.exe -d "${WSL_DISTRO_NAME}" -u root -- bash -lc "k3s ctr images import '${archive_path}'"; then
        rm -f "${archive_path}"
        return 0
      fi
      rm -f "${archive_path}"
    fi
    log "Skipping direct k3s image import because root or passwordless sudo is not available in this shell."
    return 1
  fi
  return 1
}
