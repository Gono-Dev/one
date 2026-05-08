#!/usr/bin/env bash
set -euo pipefail

APP_NAME="gono-one"
SERVICE_NAME="${GONO_ONE_SERVICE_NAME:-gono-one}"
INSTALL_URL="${GONO_ONE_INSTALL_URL:-https://run.gono.one}"

INSTALL_DIR="${GONO_ONE_INSTALL_DIR:-/opt/gono-one}"
BIN_DIR="${INSTALL_DIR}/bin"
BIN_PATH="${GONO_ONE_BIN_PATH:-${BIN_DIR}/${APP_NAME}}"
CONFIG_DIR="${GONO_ONE_CONFIG_DIR:-/etc/gono-one}"
CONFIG_FILE="${GONO_ONE_CONFIG:-${CONFIG_DIR}/config.toml}"
STATE_DIR="${GONO_ONE_STATE_DIR:-/var/lib/gono-one}"
DATA_DIR="${GONO_ONE_DATA_DIR:-${STATE_DIR}/data}"
DB_PATH="${GONO_ONE_DB_PATH:-${STATE_DIR}/gono-one.db}"
TLS_DIR="${GONO_ONE_TLS_DIR:-${CONFIG_DIR}/tls}"

RUN_USER="${GONO_ONE_USER:-gono-one}"
RUN_GROUP="${GONO_ONE_GROUP:-gono-one}"
DOMAIN="${GONO_ONE_DOMAIN:-gono.one}"
BASE_URL="${GONO_ONE_BASE_URL:-https://${DOMAIN}}"
BIND="${GONO_ONE_BIND:-127.0.0.1:16102}"
XATTR_NS="${GONO_ONE_XATTR_NS:-user.nc}"
AUTH_REALM="${GONO_ONE_AUTH_REALM:-Nextcloud}"
MAX_CONNECTIONS="${GONO_ONE_DB_MAX_CONNECTIONS:-5}"
LOG_FORMAT="${GONO_ONE_LOG_FORMAT:-text}"
RUST_LOG_VALUE="${RUST_LOG:-info}"
INSECURE_HTTP="${GONO_ONE_INSECURE_HTTP:-1}"
RELEASE_BASE="${GONO_ONE_RELEASE_BASE:-https://run.gono.one/releases}"
VERSION="${GONO_ONE_VERSION:-latest}"
BIN_URL="${GONO_ONE_BIN_URL:-}"
HEALTH_URL="${GONO_ONE_HEALTH_URL:-}"

log() {
  printf '[gono-one] %s\n' "$*"
}

warn() {
  printf '[gono-one] warning: %s\n' "$*" >&2
}

die() {
  printf '[gono-one] error: %s\n' "$*" >&2
  exit 1
}

if [[ "${EUID}" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    log "re-running installer with sudo"
    curl -fsSL "${INSTALL_URL}" | sudo -E bash
    exit $?
  fi
  die "please run as root or install sudo"
fi

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

detect_package_manager() {
  [[ -r /etc/os-release ]] || die "cannot detect OS: /etc/os-release is missing"
  # shellcheck disable=SC1091
  . /etc/os-release

  case " ${ID:-} ${ID_LIKE:-} " in
    *" debian "*|*" ubuntu "*)
      echo "apt"
      ;;
    *" rhel "*|*" centos "*|*" fedora "*)
      if command -v dnf >/dev/null 2>&1; then
        echo "dnf"
      elif command -v yum >/dev/null 2>&1; then
        echo "yum"
      else
        die "dnf or yum is required on CentOS/RHEL systems"
      fi
      ;;
    *)
      die "unsupported OS '${ID:-unknown}'. Supported: Debian, Ubuntu, CentOS/RHEL compatible"
      ;;
  esac
}

install_packages() {
  local manager="$1"
  case "${manager}" in
    apt)
      export DEBIAN_FRONTEND=noninteractive
      apt-get update
      apt-get install -y ca-certificates curl tar gzip coreutils findutils passwd systemd
      ;;
    dnf)
      dnf install -y ca-certificates curl tar gzip coreutils findutils shadow-utils systemd
      ;;
    yum)
      yum install -y ca-certificates curl tar gzip coreutils findutils shadow-utils systemd
      ;;
    *)
      die "unknown package manager: ${manager}"
      ;;
  esac
}

target_arch() {
  case "$(uname -m)" in
    x86_64|amd64)
      echo "x86_64"
      ;;
    aarch64|arm64)
      echo "aarch64"
      ;;
    *)
      die "unsupported architecture: $(uname -m)"
      ;;
  esac
}

artifact_url() {
  local arch target
  arch="$(target_arch)"
  target="linux-${arch}"

  if [[ -n "${BIN_URL}" ]]; then
    echo "${BIN_URL}"
  elif [[ "${VERSION}" == "latest" ]]; then
    echo "${RELEASE_BASE}/latest/${APP_NAME}-${target}.tar.gz"
  else
    echo "${RELEASE_BASE}/${VERSION}/${APP_NAME}-${VERSION}-${target}.tar.gz"
  fi
}

download_binary() {
  local url artifact extract_dir candidate
  url="$(artifact_url)"
  artifact="${TMP_DIR}/artifact"
  extract_dir="${TMP_DIR}/extract"

  log "downloading ${url}"
  if ! curl -fL --retry 3 --retry-delay 2 -o "${artifact}" "${url}"; then
    die "failed to download release artifact. Publish it under ${RELEASE_BASE} or set GONO_ONE_BIN_URL"
  fi

  if [[ -n "${GONO_ONE_SHA256:-}" ]]; then
    printf '%s  %s\n' "${GONO_ONE_SHA256}" "${artifact}" | sha256sum -c -
  fi

  mkdir -p "${extract_dir}"
  case "${url%%\?*}" in
    *.tar.gz|*.tgz)
      tar -xzf "${artifact}" -C "${extract_dir}"
      candidate="$(find "${extract_dir}" -type f -name "${APP_NAME}" | head -n 1)"
      [[ -n "${candidate}" ]] || die "archive does not contain ${APP_NAME}"
      install -m 0755 "${candidate}" "${BIN_PATH}"
      ;;
    *)
      install -m 0755 "${artifact}" "${BIN_PATH}"
      ;;
  esac
}

nologin_shell() {
  if [[ -x /usr/sbin/nologin ]]; then
    echo /usr/sbin/nologin
  elif [[ -x /sbin/nologin ]]; then
    echo /sbin/nologin
  else
    echo /bin/false
  fi
}

ensure_user() {
  if ! getent group "${RUN_GROUP}" >/dev/null 2>&1; then
    groupadd --system "${RUN_GROUP}"
  fi

  if ! id -u "${RUN_USER}" >/dev/null 2>&1; then
    useradd \
      --system \
      --gid "${RUN_GROUP}" \
      --home-dir "${STATE_DIR}" \
      --create-home \
      --shell "$(nologin_shell)" \
      "${RUN_USER}"
  fi
}

write_config() {
  mkdir -p "${CONFIG_DIR}" "${TLS_DIR}" "${STATE_DIR}" "${DATA_DIR}" "$(dirname "${DB_PATH}")"

  if [[ -f "${CONFIG_FILE}" ]]; then
    log "keeping existing config ${CONFIG_FILE}"
  else
    log "writing ${CONFIG_FILE}"
    cat >"${CONFIG_FILE}" <<EOF
[server]
bind = "${BIND}"
cert_file = "${TLS_DIR}/cert.pem"
key_file = "${TLS_DIR}/key.pem"
base_url = "${BASE_URL}"

[storage]
data_dir = "${DATA_DIR}"
xattr_ns = "${XATTR_NS}"

[db]
path = "${DB_PATH}"
max_connections = ${MAX_CONNECTIONS}

[auth]
realm = "${AUTH_REALM}"
EOF
    chown root:"${RUN_GROUP}" "${CONFIG_FILE}"
    chmod 0640 "${CONFIG_FILE}"
  fi

  chown -R "${RUN_USER}:${RUN_GROUP}" "${STATE_DIR}"
  chmod 0750 "${STATE_DIR}" "${DATA_DIR}"
}

write_systemd_unit() {
  local unit="/etc/systemd/system/${SERVICE_NAME}.service"
  log "writing ${unit}"
  cat >"${unit}" <<EOF
[Unit]
Description=Gono One Nextcloud-compatible WebDAV service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
Group=${RUN_GROUP}
WorkingDirectory=${STATE_DIR}
Environment=NC_DAV_CONFIG=${CONFIG_FILE}
Environment=NC_DAV_INSECURE_HTTP=${INSECURE_HTTP}
Environment=NC_DAV_LOG_FORMAT=${LOG_FORMAT}
Environment=RUST_LOG=${RUST_LOG_VALUE}
ExecStart=${BIN_PATH}
Restart=on-failure
RestartSec=5s
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=${STATE_DIR}

[Install]
WantedBy=multi-user.target
EOF
}

health_url() {
  if [[ -n "${HEALTH_URL}" ]]; then
    echo "${HEALTH_URL}"
    return
  fi

  local port="${BIND##*:}"
  echo "http://127.0.0.1:${port}/status.php"
}

wait_for_service() {
  local url="$1"
  for _ in $(seq 1 60); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if ! systemctl is-active --quiet "${SERVICE_NAME}"; then
      journalctl -u "${SERVICE_NAME}" -n 80 --no-pager >&2 || true
      die "${SERVICE_NAME} failed to start"
    fi
    sleep 1
  done

  journalctl -u "${SERVICE_NAME}" -n 80 --no-pager >&2 || true
  die "service did not become healthy at ${url}"
}

latest_generated_password() {
  local since="$1"
  journalctl -u "${SERVICE_NAME}" --since "${since}" --no-pager -o cat \
    | sed -n 's/.*Generated app password for gono: //p' \
    | tail -n 1 \
    | sed 's/[",}].*$//'
}

main() {
  require_cmd uname

  local manager start_time url password
  manager="$(detect_package_manager)"
  log "installing OS packages with ${manager}"
  install_packages "${manager}"

  require_cmd curl
  require_cmd tar
  require_cmd find
  require_cmd install
  require_cmd getent
  require_cmd useradd
  require_cmd groupadd
  require_cmd systemctl
  require_cmd journalctl

  mkdir -p "${BIN_DIR}"
  ensure_user
  download_binary
  write_config
  write_systemd_unit

  if [[ "${INSECURE_HTTP}" == "1" && "${BIND}" != 127.* && "${BIND}" != localhost:* ]]; then
    warn "NC_DAV_INSECURE_HTTP=1 with non-loopback bind '${BIND}'. Put this behind trusted network controls or enable TLS."
  fi

  start_time="$(date '+%Y-%m-%d %H:%M:%S')"
  systemctl daemon-reload
  systemctl enable "${SERVICE_NAME}" >/dev/null
  systemctl restart "${SERVICE_NAME}"

  url="$(health_url)"
  wait_for_service "${url}"

  password="$(latest_generated_password "${start_time}" || true)"

  log "installed ${APP_NAME}"
  log "service: systemctl status ${SERVICE_NAME}"
  log "local health: ${url}"
  log "public base URL: ${BASE_URL}"
  log "webdav URL: ${BASE_URL}/remote.php/dav"
  if [[ -n "${password}" ]]; then
    log "bootstrap user: gono"
    log "bootstrap app password: ${password}"
    warn "save this password now; normal restarts will not print it again"
  else
    log "no new bootstrap password was printed; existing database/password was preserved"
  fi
  log "configure HTTPS reverse proxy to forward ${BASE_URL} to http://${BIND}"
}

main "$@"
