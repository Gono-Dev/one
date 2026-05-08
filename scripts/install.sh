#!/usr/bin/env bash
set -euo pipefail

APP_NAME="gono-one"
INSTALL_URL="${GONO_ONE_INSTALL_URL:-https://run.gono.one}"

PLATFORM=""
PACKAGE_MANAGER=""

SERVICE_NAME=""
INSTALL_DIR=""
BIN_DIR=""
BIN_PATH=""
CONFIG_DIR=""
CONFIG_FILE=""
STATE_DIR=""
DATA_DIR=""
DB_PATH=""
TLS_DIR=""
LOG_DIR=""
PLIST_PATH=""

RUN_USER=""
RUN_GROUP=""
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

LOG_STDOUT_OFFSET=0
LOG_STDERR_OFFSET=0

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

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

detect_platform() {
  case "$(uname -s)" in
    Linux)
      echo "linux"
      ;;
    Darwin)
      echo "macos"
      ;;
    *)
      die "unsupported OS kernel: $(uname -s). Supported: macOS, Debian, Ubuntu, CentOS/RHEL compatible"
      ;;
  esac
}

detect_linux_package_manager() {
  [[ -r /etc/os-release ]] || die "cannot detect Linux distribution: /etc/os-release is missing"
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
      die "unsupported Linux distribution '${ID:-unknown}'. Supported: Debian, Ubuntu, CentOS/RHEL compatible"
      ;;
  esac
}

set_platform_defaults() {
  PLATFORM="$(detect_platform)"

  case "${PLATFORM}" in
    linux)
      PACKAGE_MANAGER="$(detect_linux_package_manager)"
      SERVICE_NAME="${GONO_ONE_SERVICE_NAME:-gono-one}"
      INSTALL_DIR="${GONO_ONE_INSTALL_DIR:-/opt/gono-one}"
      CONFIG_DIR="${GONO_ONE_CONFIG_DIR:-/etc/gono-one}"
      STATE_DIR="${GONO_ONE_STATE_DIR:-/var/lib/gono-one}"
      LOG_DIR="${GONO_ONE_LOG_DIR:-/var/log/gono-one}"
      RUN_USER="${GONO_ONE_USER:-gono-one}"
      RUN_GROUP="${GONO_ONE_GROUP:-gono-one}"
      ;;
    macos)
      PACKAGE_MANAGER="none"
      SERVICE_NAME="${GONO_ONE_SERVICE_NAME:-one.gono.gono-one}"
      INSTALL_DIR="${GONO_ONE_INSTALL_DIR:-/opt/gono-one}"
      CONFIG_DIR="${GONO_ONE_CONFIG_DIR:-/Library/Application Support/Gono One}"
      STATE_DIR="${GONO_ONE_STATE_DIR:-/Library/Application Support/Gono One}"
      LOG_DIR="${GONO_ONE_LOG_DIR:-/Library/Logs/Gono One}"
      RUN_USER="${GONO_ONE_USER:-root}"
      RUN_GROUP="${GONO_ONE_GROUP:-wheel}"
      ;;
    *)
      die "unsupported platform: ${PLATFORM}"
      ;;
  esac

  BIN_DIR="${INSTALL_DIR}/bin"
  BIN_PATH="${GONO_ONE_BIN_PATH:-${BIN_DIR}/${APP_NAME}}"
  CONFIG_FILE="${GONO_ONE_CONFIG:-${CONFIG_DIR}/config.toml}"
  DATA_DIR="${GONO_ONE_DATA_DIR:-${STATE_DIR}/data}"
  DB_PATH="${GONO_ONE_DB_PATH:-${STATE_DIR}/gono-one.db}"
  TLS_DIR="${GONO_ONE_TLS_DIR:-${CONFIG_DIR}/tls}"
  PLIST_PATH="${GONO_ONE_PLIST_PATH:-/Library/LaunchDaemons/${SERVICE_NAME}.plist}"
}

require_root() {
  if [[ "${EUID}" -eq 0 ]]; then
    return
  fi

  if command -v sudo >/dev/null 2>&1; then
    log "re-running installer with sudo"
    curl -fsSL "${INSTALL_URL}" | sudo -E bash
    exit $?
  fi

  die "please run as root or install sudo"
}

install_packages() {
  case "${PLATFORM}:${PACKAGE_MANAGER}" in
    linux:apt)
      export DEBIAN_FRONTEND=noninteractive
      apt-get update
      apt-get install -y ca-certificates curl tar gzip coreutils findutils passwd systemd
      ;;
    linux:dnf)
      dnf install -y ca-certificates curl tar gzip coreutils findutils shadow-utils systemd
      ;;
    linux:yum)
      yum install -y ca-certificates curl tar gzip coreutils findutils shadow-utils systemd
      ;;
    macos:none)
      log "macOS detected; using built-in curl, tar, launchd, and system tools"
      ;;
    *)
      die "unknown package manager: ${PACKAGE_MANAGER}"
      ;;
  esac
}

require_platform_commands() {
  require_cmd curl
  require_cmd tar
  require_cmd find
  require_cmd install
  require_cmd sed
  require_cmd tail

  case "${PLATFORM}" in
    linux)
      require_cmd getent
      require_cmd useradd
      require_cmd groupadd
      require_cmd systemctl
      require_cmd journalctl
      ;;
    macos)
      require_cmd launchctl
      require_cmd id
      ;;
  esac
}

target_arch() {
  local machine
  machine="${GONO_ONE_ARCH:-$(uname -m)}"

  case "${machine}" in
    x86_64|amd64)
      echo "x86_64"
      ;;
    aarch64|arm64)
      echo "aarch64"
      ;;
    armv8l|armv7l|armv7|armv7hf|armhf)
      if [[ "${PLATFORM}" == "macos" ]]; then
        die "unsupported macOS ARM architecture: ${machine}. macOS ARM builds must be aarch64/arm64."
      fi
      echo "armv7"
      ;;
    armv6l|armv6)
      if [[ "${PLATFORM}" == "macos" ]]; then
        die "unsupported macOS ARM architecture: ${machine}. macOS ARM builds must be aarch64/arm64."
      fi
      echo "armv6"
      ;;
    *)
      die "unsupported architecture: ${machine}. Supported: x86_64/amd64, aarch64/arm64, Linux armv7, Linux armv6"
      ;;
  esac
}

target_os() {
  case "${PLATFORM}" in
    linux)
      echo "linux"
      ;;
    macos)
      echo "macos"
      ;;
    *)
      die "unsupported platform: ${PLATFORM}"
      ;;
  esac
}

artifact_url() {
  local arch os target
  arch="$(target_arch)"
  os="$(target_os)"
  target="${os}-${arch}"

  if [[ -n "${BIN_URL}" ]]; then
    echo "${BIN_URL}"
  elif [[ "${VERSION}" == "latest" ]]; then
    echo "${RELEASE_BASE}/latest/${APP_NAME}-${target}.tar.gz"
  else
    echo "${RELEASE_BASE}/${VERSION}/${APP_NAME}-${VERSION}-${target}.tar.gz"
  fi
}

verify_sha256() {
  local expected="$1"
  local file="$2"
  local actual

  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s  %s\n' "${expected}" "${file}" | sha256sum -c -
  elif command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "${file}" | while read -r hash _; do echo "${hash}"; done)"
    [[ "${actual}" == "${expected}" ]] || die "sha256 mismatch: expected ${expected}, got ${actual}"
  else
    die "GONO_ONE_SHA256 was set but neither sha256sum nor shasum is available"
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
    verify_sha256 "${GONO_ONE_SHA256}" "${artifact}"
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

ensure_linux_user() {
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

ensure_macos_user() {
  if ! id -u "${RUN_USER}" >/dev/null 2>&1; then
    die "macOS run user '${RUN_USER}' does not exist. Use GONO_ONE_USER=root or create the user first."
  fi
}

ensure_run_identity() {
  case "${PLATFORM}" in
    linux)
      ensure_linux_user
      ;;
    macos)
      ensure_macos_user
      ;;
  esac
}

prepare_directories() {
  mkdir -p "${BIN_DIR}" "${CONFIG_DIR}" "${TLS_DIR}" "${STATE_DIR}" "${DATA_DIR}" "$(dirname "${DB_PATH}")" "${LOG_DIR}"
  chown -R "${RUN_USER}:${RUN_GROUP}" "${STATE_DIR}" "${LOG_DIR}"
  chmod 0750 "${STATE_DIR}" "${DATA_DIR}"
  chmod 0755 "${LOG_DIR}"
}

write_config() {
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
ReadWritePaths=${STATE_DIR} ${LOG_DIR}

[Install]
WantedBy=multi-user.target
EOF
}

xml_escape() {
  local value="$1"
  value="${value//&/&amp;}"
  value="${value//</&lt;}"
  value="${value//>/&gt;}"
  printf '%s' "${value}"
}

write_launchd_plist() {
  log "writing ${PLIST_PATH}"
  cat >"${PLIST_PATH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$(xml_escape "${SERVICE_NAME}")</string>
    <key>ProgramArguments</key>
    <array>
        <string>$(xml_escape "${BIN_PATH}")</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$(xml_escape "${STATE_DIR}")</string>
    <key>UserName</key>
    <string>$(xml_escape "${RUN_USER}")</string>
    <key>GroupName</key>
    <string>$(xml_escape "${RUN_GROUP}")</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>NC_DAV_CONFIG</key>
        <string>$(xml_escape "${CONFIG_FILE}")</string>
        <key>NC_DAV_INSECURE_HTTP</key>
        <string>$(xml_escape "${INSECURE_HTTP}")</string>
        <key>NC_DAV_LOG_FORMAT</key>
        <string>$(xml_escape "${LOG_FORMAT}")</string>
        <key>RUST_LOG</key>
        <string>$(xml_escape "${RUST_LOG_VALUE}")</string>
    </dict>
    <key>StandardOutPath</key>
    <string>$(xml_escape "${LOG_DIR}/stdout.log")</string>
    <key>StandardErrorPath</key>
    <string>$(xml_escape "${LOG_DIR}/stderr.log")</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
</dict>
</plist>
EOF
  chown root:wheel "${PLIST_PATH}"
  chmod 0644 "${PLIST_PATH}"
}

write_service_definition() {
  case "${PLATFORM}" in
    linux)
      write_systemd_unit
      ;;
    macos)
      write_launchd_plist
      ;;
  esac
}

health_url() {
  if [[ -n "${HEALTH_URL}" ]]; then
    echo "${HEALTH_URL}"
    return
  fi

  local port="${BIND##*:}"
  echo "http://127.0.0.1:${port}/status.php"
}

show_service_logs() {
  case "${PLATFORM}" in
    linux)
      journalctl -u "${SERVICE_NAME}" -n 80 --no-pager >&2 || true
      ;;
    macos)
      launchctl print "system/${SERVICE_NAME}" >&2 2>/dev/null || true
      tail -n 80 "${LOG_DIR}/stderr.log" >&2 2>/dev/null || true
      tail -n 80 "${LOG_DIR}/stdout.log" >&2 2>/dev/null || true
      ;;
  esac
}

service_is_active() {
  case "${PLATFORM}" in
    linux)
      systemctl is-active --quiet "${SERVICE_NAME}"
      ;;
    macos)
      launchctl print "system/${SERVICE_NAME}" >/dev/null 2>&1
      ;;
  esac
}

wait_for_service() {
  local url="$1"
  for _ in $(seq 1 60); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if ! service_is_active; then
      show_service_logs
      die "${SERVICE_NAME} failed to start"
    fi
    sleep 1
  done

  show_service_logs
  die "service did not become healthy at ${url}"
}

log_size() {
  local file="$1"
  if [[ -f "${file}" ]]; then
    wc -c <"${file}" | tr -d '[:space:]'
  else
    echo 0
  fi
}

capture_macos_log_offsets() {
  LOG_STDOUT_OFFSET="$(log_size "${LOG_DIR}/stdout.log")"
  LOG_STDERR_OFFSET="$(log_size "${LOG_DIR}/stderr.log")"
}

latest_generated_password() {
  local since="$1"

  case "${PLATFORM}" in
    linux)
      journalctl -u "${SERVICE_NAME}" --since "${since}" --no-pager -o cat \
        | sed -n 's/.*Generated app password for gono: //p' \
        | tail -n 1 \
        | sed 's/[",}].*$//'
      ;;
    macos)
      {
        tail -c +"$((LOG_STDOUT_OFFSET + 1))" "${LOG_DIR}/stdout.log" 2>/dev/null || true
        tail -c +"$((LOG_STDERR_OFFSET + 1))" "${LOG_DIR}/stderr.log" 2>/dev/null || true
      } \
        | sed -n 's/.*Generated app password for gono: //p' \
        | tail -n 1 \
        | sed 's/[",}].*$//'
      ;;
  esac
}

start_linux_service() {
  systemctl daemon-reload
  systemctl enable "${SERVICE_NAME}" >/dev/null
  systemctl restart "${SERVICE_NAME}"
}

start_macos_service() {
  touch "${LOG_DIR}/stdout.log" "${LOG_DIR}/stderr.log"
  chown "${RUN_USER}:${RUN_GROUP}" "${LOG_DIR}/stdout.log" "${LOG_DIR}/stderr.log"
  capture_macos_log_offsets

  launchctl bootout system "${PLIST_PATH}" >/dev/null 2>&1 || true
  if ! launchctl bootstrap system "${PLIST_PATH}"; then
    warn "launchctl bootstrap failed; trying legacy launchctl load"
    launchctl unload "${PLIST_PATH}" >/dev/null 2>&1 || true
    launchctl load -w "${PLIST_PATH}"
  fi
  launchctl enable "system/${SERVICE_NAME}" >/dev/null 2>&1 || true
  launchctl kickstart -k "system/${SERVICE_NAME}" >/dev/null 2>&1 || true
}

start_service() {
  case "${PLATFORM}" in
    linux)
      start_linux_service
      ;;
    macos)
      start_macos_service
      ;;
  esac
}

service_status_hint() {
  case "${PLATFORM}" in
    linux)
      echo "systemctl status ${SERVICE_NAME}"
      ;;
    macos)
      echo "launchctl print system/${SERVICE_NAME}"
      ;;
  esac
}

service_log_hint() {
  case "${PLATFORM}" in
    linux)
      echo "journalctl -u ${SERVICE_NAME} --no-pager"
      ;;
    macos)
      echo "tail -f '${LOG_DIR}/stdout.log' '${LOG_DIR}/stderr.log'"
      ;;
  esac
}

main() {
  require_cmd uname
  set_platform_defaults
  require_root

  local start_time url password
  log "installing for ${PLATFORM}"
  install_packages
  require_platform_commands

  ensure_run_identity
  prepare_directories
  download_binary
  write_config
  write_service_definition

  if [[ "${INSECURE_HTTP}" == "1" && "${BIND}" != 127.* && "${BIND}" != localhost:* ]]; then
    warn "NC_DAV_INSECURE_HTTP=1 with non-loopback bind '${BIND}'. Put this behind trusted network controls or enable TLS."
  fi

  start_time="$(date '+%Y-%m-%d %H:%M:%S')"
  start_service

  url="$(health_url)"
  wait_for_service "${url}"

  password="$(latest_generated_password "${start_time}" || true)"

  log "installed ${APP_NAME}"
  log "service: $(service_status_hint)"
  log "logs: $(service_log_hint)"
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

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

main "$@"
