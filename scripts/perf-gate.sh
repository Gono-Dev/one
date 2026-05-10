#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCENARIO="${1:-baseline}"
if [[ "$#" -gt 0 ]]; then
  shift
fi

PROFILE="${GONO_PERF_PROFILE:-medium}"
USER_NAME="${GONO_PERF_USER:-gono}"
REPORT_ROOT="${GONO_PERF_REPORT_ROOT:-${ROOT}/target/perf-reports}"
RUN_ID="${GONO_PERF_RUN_ID:-$(date '+%Y%m%d-%H%M%S')}"
REPORT_DIR="${GONO_PERF_REPORT_DIR:-${REPORT_ROOT}/${RUN_ID}}"

log() {
  printf '[gono-cloud-perf] %s\n' "$*"
}

die() {
  printf '[gono-cloud-perf] error: %s\n' "$*" >&2
  exit 1
}

detect_config_file() {
  if [[ -n "${GONO_PERF_CONFIG:-}" ]]; then
    printf '%s\n' "${GONO_PERF_CONFIG}"
    return
  fi
  if [[ -r "/Library/Application Support/Gono Cloud/config.toml" ]]; then
    printf '%s\n' "/Library/Application Support/Gono Cloud/config.toml"
    return
  fi
  if [[ -r "/etc/gono-cloud/config.toml" ]]; then
    printf '%s\n' "/etc/gono-cloud/config.toml"
    return
  fi
  printf '%s\n' ""
}

detect_binary() {
  if [[ -n "${GONO_PERF_BIN:-}" ]]; then
    printf '%s\n' "${GONO_PERF_BIN}"
    return
  fi
  if [[ -x "/opt/gono-cloud/bin/gono-cloud" ]]; then
    printf '%s\n' "/opt/gono-cloud/bin/gono-cloud"
    return
  fi
  if [[ -x "${ROOT}/target/release/gono-cloud" ]]; then
    printf '%s\n' "${ROOT}/target/release/gono-cloud"
    return
  fi
  if [[ -x "${ROOT}/target/debug/gono-cloud" ]]; then
    printf '%s\n' "${ROOT}/target/debug/gono-cloud"
    return
  fi
  printf '%s\n' ""
}

extract_toml_string() {
  local file="$1"
  local section="$2"
  local key="$3"
  awk -v section="${section}" -v key="${key}" '
    $0 ~ /^[[:space:]]*\[/ {
      in_section = ($0 ~ "^[[:space:]]*\\[" section "\\][[:space:]]*$")
      next
    }
    in_section {
      line = $0
      sub(/[[:space:]]*#.*/, "", line)
      if (line ~ "^[[:space:]]*" key "[[:space:]]*=") {
        sub(/^[^=]*=[[:space:]]*/, "", line)
        sub(/^[[:space:]]*"/, "", line)
        sub(/"[[:space:]]*$/, "", line)
        print line
        exit
      }
    }
  ' "${file}"
}

detect_base_url() {
  if [[ -n "${GONO_PERF_BASE_URL:-}" ]]; then
    printf '%s\n' "${GONO_PERF_BASE_URL}"
    return
  fi
  if [[ -n "${CONFIG_FILE}" && -r "${CONFIG_FILE}" ]]; then
    local value
    value="$(extract_toml_string "${CONFIG_FILE}" server base_url || true)"
    if [[ -n "${value}" ]]; then
      printf '%s\n' "${value%/}"
      return
    fi
  fi
  printf '%s\n' "http://127.0.0.1:16102"
}

create_app_password() {
  local bin_path="$1"
  local config_file="$2"
  local label="perf-${RUN_ID}"
  local output password

  [[ -x "${bin_path}" ]] || die "cannot create app password; binary is not executable: ${bin_path}"
  [[ -r "${config_file}" ]] || die "cannot create app password; config is not readable: ${config_file}"

  log "creating temporary app password label ${label} for ${USER_NAME}"
  output="$(GONE_CLOUD_CONFIG="${config_file}" "${bin_path}" app-password-add "${USER_NAME}" "${label}" --mount /=/:full)"
  password="$(printf '%s\n' "${output}" | sed -n 's/^App password: //p' | tail -n 1)"
  [[ -n "${password}" ]] || die "failed to parse generated app password"
  printf '%s\n' "${password}"
}

CONFIG_FILE="$(detect_config_file)"
BIN_PATH="$(detect_binary)"
BASE_URL="$(detect_base_url)"

if [[ "${GONO_PERF_CREATE_PASSWORD:-0}" == "1" ]]; then
  GONO_PERF_PASSWORD="$(create_app_password "${BIN_PATH}" "${CONFIG_FILE}")"
  export GONO_PERF_PASSWORD
fi

if [[ -z "${GONO_PERF_PASSWORD:-}" ]]; then
  cat >&2 <<EOF
Missing GONO_PERF_PASSWORD.

Run with an existing app password:
  GONO_PERF_BASE_URL=${BASE_URL} GONO_PERF_USER=${USER_NAME} GONO_PERF_PASSWORD=... scripts/perf-gate.sh ${SCENARIO}

Or let the script create a temporary full-scope app password through the local binary:
  sudo GONO_PERF_CREATE_PASSWORD=1 scripts/perf-gate.sh ${SCENARIO}
EOF
  exit 1
fi

mkdir -p "${REPORT_DIR}"

log "scenario: ${SCENARIO}"
log "profile: ${PROFILE}"
log "base URL: ${BASE_URL}"
log "user: ${USER_NAME}"
log "config: ${CONFIG_FILE:-not found}"
log "reports: ${REPORT_DIR}"

args=(
  "${SCENARIO}"
  --profile "${PROFILE}"
  --base-url "${BASE_URL}"
  --user "${USER_NAME}"
  --report-dir "${REPORT_DIR}"
)
if [[ -n "${CONFIG_FILE}" ]]; then
  args+=(--config "${CONFIG_FILE}")
fi

python3 "${ROOT}/scripts/perf-gate.py" "${args[@]}" "$@"
