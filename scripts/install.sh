#!/usr/bin/env bash
set -euo pipefail

APP_NAME="gono-cloud"
INSTALL_URL="https://run.gono.cloud"
RELEASE_REPO="Gono-Dev/cloud.server"
RELEASE_BASE="https://github.com/${RELEASE_REPO}/releases"

INTERNAL_ACTION="${INSTALLER_INTERNAL_ACTION:-}"
INTERNAL_LOCAL_BIN="${INSTALLER_INTERNAL_LOCAL_BIN:-}"

SCRIPT_PATH=""
SCRIPT_DIR=""
REPO_ROOT=""

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

DOMAIN="gono.cloud"
BASE_URL="https://${DOMAIN}"
BIND="127.0.0.1:16102"
XATTR_NS="user.nc"
AUTH_REALM="Gono Cloud"
ADMIN_ENABLED="false"
ADMIN_USERS=""
MAX_CONNECTIONS="5"
LOG_FORMAT="text"
RUST_LOG_VALUE="info"
INSECURE_HTTP="1"
LOCAL_BIN=""
PRESERVE_CONFIG="0"
PURGE="0"

LOG_STDOUT_OFFSET=0
LOG_STDERR_OFFSET=0

log() {
  printf '[gono-cloud] %s\n' "$*"
}

warn() {
  printf '[gono-cloud] warning: %s\n' "$*" >&2
}

die() {
  printf '[gono-cloud] error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

usage_name() {
  if [[ -n "${SCRIPT_PATH}" && -n "${REPO_ROOT}" && "${SCRIPT_PATH}" == "${REPO_ROOT}/"* ]]; then
    printf '%s\n' "${SCRIPT_PATH#"${REPO_ROOT}/"}"
  elif [[ -n "${SCRIPT_PATH}" ]]; then
    printf '%s\n' "${SCRIPT_PATH}"
  else
    printf '%s\n' "bash <(curl -sL ${INSTALL_URL})"
  fi
}

show_help() {
  cat <<EOF
Gono Cloud interactive installer

Run without arguments:
  $(usage_name)

Menu actions:
  1. Install or upgrade Gono Cloud
  2. Restart service
  3. Uninstall Gono Cloud
  4. Show service status
  5. Follow service logs
  6. Help
  0. Exit

Notes:
  - Command-line install/status/logs/restart/uninstall options are no longer supported.
  - Install or upgrade automatically uses a local repository binary/build when available.
  - Remote installs download the latest release artifact from GitHub.
  - Existing config files are preserved by default during upgrades.
  - Local accounts are handled by the Web Admin UI or the gono-cloud binary CLI.
EOF
}

reject_args() {
  warn "command-line arguments are no longer supported by this installer"
  warn "run without arguments and choose an action from the interactive menu"
  show_help >&2
  exit 1
}

sudo_run_script() {
  local script="$1"
  shift
  if [[ -r /dev/tty ]]; then
    sudo env "$@" bash "${script}" </dev/tty
  else
    sudo env "$@" bash "${script}"
  fi
}

resolve_script_context() {
  local source
  source="${BASH_SOURCE[0]:-$0}"

  case "${source}" in
    ""|-|bash|/dev/fd/*|/proc/self/fd/*)
      return
      ;;
  esac

  [[ -e "${source}" ]] || return

  case "${source}" in
    /*)
      SCRIPT_PATH="${source}"
      ;;
    *)
      SCRIPT_PATH="$(cd "$(dirname "${source}")" && pwd -P)/$(basename "${source}")"
      ;;
  esac

  SCRIPT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")" && pwd -P)"
  if [[ -f "${SCRIPT_DIR}/../Cargo.toml" && -d "${SCRIPT_DIR}/../src" ]]; then
    REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
  fi
}

download_script_for_sudo() {
  local sudo_script="${TMP_DIR}/gono-cloud-install.sh"
  require_cmd curl
  curl -fsSL "${INSTALL_URL}" -o "${sudo_script}"
  chmod 0700 "${sudo_script}"
  printf '%s\n' "${sudo_script}"
}

run_action_as_root() {
  local action="$1"
  local script
  local -a env_args

  if [[ "${EUID}" -eq 0 ]]; then
    dispatch_action "${action}"
    return
  fi

  if [[ "${action}" == "install" ]]; then
    prepare_local_binary_before_sudo
  fi

  command -v sudo >/dev/null 2>&1 || die "please run as root or install sudo"

  env_args=("INSTALLER_INTERNAL_ACTION=${action}")
  if [[ -n "${LOCAL_BIN}" ]]; then
    env_args+=("INSTALLER_INTERNAL_LOCAL_BIN=${LOCAL_BIN}")
  fi

  log "re-running selected action with sudo"
  if [[ -n "${SCRIPT_PATH}" && -r "${SCRIPT_PATH}" ]]; then
    sudo_run_script "${SCRIPT_PATH}" "${env_args[@]}"
  else
    script="$(download_script_for_sudo)"
    sudo_run_script "${script}" "${env_args[@]}"
  fi
  exit $?
}

prompt_text() {
  local prompt="$1"
  local default="$2"
  local input

  if [[ -n "${default}" ]]; then
    printf "%s [%s]: " "${prompt}" "${default}" >&2
  else
    printf "%s: " "${prompt}" >&2
  fi
  if ! read -r input; then
    die "failed to read input"
  fi
  if [[ -z "${input}" ]]; then
    printf '%s\n' "${default}"
  else
    printf '%s\n' "${input}"
  fi
}

prompt_yes_no() {
  local prompt="$1"
  local default="$2"
  local input suffix

  case "${default}" in
    y|Y|yes|YES|true|1)
      suffix="[Y/n]"
      default="y"
      ;;
    *)
      suffix="[y/N]"
      default="n"
      ;;
  esac

  while true; do
    printf "%s %s " "${prompt}" "${suffix}" >&2
    if ! read -r input; then
      die "failed to read input"
    fi
    case "${input:-${default}}" in
      y|Y|yes|YES|Yes)
        return 0
        ;;
      n|N|no|NO|No)
        return 1
        ;;
      *)
        warn "please answer yes or no"
        ;;
    esac
  done
}

show_interactive_menu() {
  local choice

  while true; do
    cat <<EOF
Gono Cloud installer

Please select an action:
  1. Install or upgrade Gono Cloud
  2. Restart service
  3. Uninstall Gono Cloud
  4. Show service status
  5. Show service logs
  6. Help
  0. Exit
EOF
    printf "Enter choice [1-6,0]: "
    if ! read -r choice; then
      die "failed to read menu selection"
    fi

    case "${choice}" in
      1|install|i|I)
        run_action_as_root "install"
        ;;
      2|restart|r|R)
        run_action_as_root "restart"
        ;;
      3|uninstall|remove|u|U)
        run_action_as_root "uninstall"
        ;;
      4|status|s|S)
        run_action_as_root "status"
        ;;
      5|logs|log|l|L)
        run_action_as_root "logs"
        ;;
      6|help|h|H|\?)
        show_help
        ;;
      0|exit|quit|q|Q)
        log "cancelled"
        exit 0
        ;;
      *)
        warn "invalid selection: ${choice:-<empty>}"
        ;;
    esac
    printf '\n'
  done
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

extract_toml_bool() {
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
        gsub(/[[:space:]]/, "", line)
        print line
        exit
      }
    }
  ' "${file}"
}

load_existing_config_defaults() {
  local value
  [[ -r "${CONFIG_FILE}" ]] || return 0

  value="$(extract_toml_string "${CONFIG_FILE}" server bind || true)"
  [[ -n "${value}" ]] && BIND="${value}"
  value="$(extract_toml_string "${CONFIG_FILE}" server base_url || true)"
  [[ -n "${value}" ]] && BASE_URL="${value}"
  value="$(extract_toml_string "${CONFIG_FILE}" storage data_dir || true)"
  [[ -n "${value}" ]] && DATA_DIR="${value}"
  value="$(extract_toml_string "${CONFIG_FILE}" storage xattr_ns || true)"
  [[ -n "${value}" ]] && XATTR_NS="${value}"
  value="$(extract_toml_string "${CONFIG_FILE}" db path || true)"
  [[ -n "${value}" ]] && DB_PATH="${value}"
  value="$(extract_toml_string "${CONFIG_FILE}" auth realm || true)"
  [[ -n "${value}" ]] && AUTH_REALM="${value}"
  value="$(extract_toml_bool "${CONFIG_FILE}" admin enabled || true)"
  [[ -n "${value}" ]] && ADMIN_ENABLED="${value}"
}

inline_table_header_lines() {
  local file="$1"
  awk '
    /^[[:space:]]*\[[^]]+\][[:space:]]+[^#[:space:]]/ {
      printf "%s:%s\n", NR, $0
    }
  ' "${file}"
}

validate_existing_config_for_preserve() {
  local invalid_lines
  invalid_lines="$(inline_table_header_lines "${CONFIG_FILE}")"
  if [[ -n "${invalid_lines}" ]]; then
    warn "existing config is not valid TOML; table headers such as [server] must be on their own line"
    printf '%s\n' "${invalid_lines}" >&2
    return 1
  fi
  return 0
}

set_platform_defaults() {
  PLATFORM="$(detect_platform)"

  case "${PLATFORM}" in
    linux)
      PACKAGE_MANAGER="$(detect_linux_package_manager)"
      SERVICE_NAME="gono-cloud"
      INSTALL_DIR="/opt/gono-cloud"
      CONFIG_DIR="/etc/gono-cloud"
      STATE_DIR="/var/lib/gono-cloud"
      LOG_DIR="/var/log/gono-cloud"
      RUN_USER="gono-cloud"
      RUN_GROUP="gono-cloud"
      ;;
    macos)
      PACKAGE_MANAGER="none"
      SERVICE_NAME="cloud.gono.gono-cloud"
      INSTALL_DIR="/opt/gono-cloud"
      CONFIG_DIR="/Library/Application Support/Gono Cloud"
      STATE_DIR="/Library/Application Support/Gono Cloud"
      LOG_DIR="/Library/Logs/Gono Cloud"
      RUN_USER="root"
      RUN_GROUP="wheel"
      ;;
    *)
      die "unsupported platform: ${PLATFORM}"
      ;;
  esac

  BIN_DIR="${INSTALL_DIR}/bin"
  BIN_PATH="${BIN_DIR}/${APP_NAME}"
  CONFIG_FILE="${CONFIG_DIR}/config.toml"
  DATA_DIR="${STATE_DIR}/data"
  DB_PATH="${STATE_DIR}/gono-cloud.db"
  TLS_DIR="${CONFIG_DIR}/tls"
  PLIST_PATH="/Library/LaunchDaemons/${SERVICE_NAME}.plist"
  load_existing_config_defaults
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
  machine="$(uname -m)"

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
  echo "${RELEASE_BASE}/latest/download/${APP_NAME}-${target}.tar.gz"
}

target_dir() {
  printf '%s\n' "${REPO_ROOT}/target"
}

local_binary_candidate() {
  printf '%s/release/%s\n' "$(target_dir)" "${APP_NAME}"
}

build_local_binary() {
  [[ -n "${REPO_ROOT}" ]] || die "cannot build local binary: repository root was not detected"
  require_cmd cargo
  log "building local release binary from ${REPO_ROOT}" >&2
  cargo build --locked --release --manifest-path "${REPO_ROOT}/Cargo.toml"
}

prepare_local_binary_before_sudo() {
  local candidate
  [[ -n "${REPO_ROOT}" ]] || return 0

  candidate="$(local_binary_candidate)"
  if [[ -x "${candidate}" ]]; then
    LOCAL_BIN="${candidate}"
    return
  fi

  if command -v cargo >/dev/null 2>&1; then
    build_local_binary
    if [[ -x "${candidate}" ]]; then
      LOCAL_BIN="${candidate}"
    fi
  else
    warn "cargo is not available; sudo install will fall back to the latest release artifact"
  fi
}

use_local_source() {
  local candidate

  if [[ -n "${LOCAL_BIN}" ]]; then
    [[ -x "${LOCAL_BIN}" ]] || die "local binary is not executable: ${LOCAL_BIN}"
    return 0
  fi

  [[ -n "${REPO_ROOT}" ]] || return 1

  candidate="$(local_binary_candidate)"
  if [[ -x "${candidate}" ]]; then
    LOCAL_BIN="${candidate}"
    return 0
  fi

  if command -v cargo >/dev/null 2>&1; then
    build_local_binary
    [[ -x "${candidate}" ]] || die "local build did not produce ${candidate}"
    LOCAL_BIN="${candidate}"
    return 0
  fi

  return 1
}

sha256_from_sidecar() {
  local file="$1"
  sed -n '1s/^\([0-9a-fA-F]\{64\}\).*/\1/p' "${file}"
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
    die "neither sha256sum nor shasum is available"
  fi
}

verify_downloaded_artifact() {
  local url="$1"
  local artifact="$2"
  local sidecar sidecar_url expected

  sidecar="${TMP_DIR}/artifact.sha256"
  sidecar_url="${url%%\?*}.sha256"
  if curl -fsL --retry 3 --retry-delay 2 -o "${sidecar}" "${sidecar_url}"; then
    expected="$(sha256_from_sidecar "${sidecar}")"
    [[ -n "${expected}" ]] || die "sha256 sidecar does not contain a hash: ${sidecar_url}"
    verify_sha256 "${expected}" "${artifact}"
  else
    warn "sha256 sidecar was not found at ${sidecar_url}; continuing without checksum verification"
  fi
}

download_binary() {
  local url artifact extract_dir candidate
  url="$(artifact_url)"
  artifact="${TMP_DIR}/artifact"
  extract_dir="${TMP_DIR}/extract"

  log "downloading ${url}"
  if ! curl -fL --retry 3 --retry-delay 2 -o "${artifact}" "${url}"; then
    die "failed to download release artifact from ${url}"
  fi

  verify_downloaded_artifact "${url}" "${artifact}"

  mkdir -p "${extract_dir}"
  case "${url%%\?*}" in
    *.tar.gz|*.tgz)
      tar -xzf "${artifact}" -C "${extract_dir}"
      candidate="$(find "${extract_dir}" -type f -name "${APP_NAME}" | sed -n '1p')"
      [[ -n "${candidate}" ]] || die "archive does not contain ${APP_NAME}"
      install -m 0755 "${candidate}" "${BIN_PATH}"
      ;;
    *)
      install -m 0755 "${artifact}" "${BIN_PATH}"
      ;;
  esac
}

install_binary() {
  if use_local_source; then
    log "installing local binary ${LOCAL_BIN}"
    install -m 0755 "${LOCAL_BIN}" "${BIN_PATH}"
  else
    download_binary
  fi
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
    die "macOS run user '${RUN_USER}' does not exist"
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
  case "${PLATFORM}" in
    macos)
      chown -R "${RUN_USER}:${RUN_GROUP}" "${STATE_DIR}" "${LOG_DIR}"
      chmod 0755 "${STATE_DIR}" "${LOG_DIR}"
      chmod 0750 "${DATA_DIR}"
      ;;
    linux)
      chown -R "${RUN_USER}:${RUN_GROUP}" "${STATE_DIR}" "${LOG_DIR}"
      chmod 0750 "${STATE_DIR}" "${DATA_DIR}"
      chmod 0755 "${LOG_DIR}"
      ;;
  esac
}

toml_string_array_from_csv() {
  local csv="$1"
  local IFS=,
  local item trimmed first
  local output="["
  first=1

  for item in ${csv}; do
    trimmed="${item//[[:space:]]/}"
    [[ -n "${trimmed}" ]] || continue
    trimmed="${trimmed//\\/\\\\}"
    trimmed="${trimmed//\"/\\\"}"
    if [[ "${first}" == "1" ]]; then
      first=0
    else
      output+=", "
    fi
    output+="\"${trimmed}\""
  done

  output+="]"
  printf '%s\n' "${output}"
}

toml_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '%s' "${value}"
}

normalize_base_url_input() {
  local value="$1"
  case "${value}" in
    http://*|https://*)
      ;;
    *)
      value="https://${value}"
      ;;
  esac

  while [[ "${value}" == */ ]]; do
    value="${value%/}"
  done
  printf '%s\n' "${value}"
}

host_from_base_url() {
  local value="$1"
  value="${value#http://}"
  value="${value#https://}"
  value="${value%%/*}"
  printf '%s\n' "${value}"
}

prompt_base_url() {
  local value host

  while true; do
    value="$(prompt_text "Public base URL (domain is OK)" "${BASE_URL}")"
    value="$(normalize_base_url_input "${value}")"
    host="$(host_from_base_url "${value}")"
    if [[ -n "${host}" ]]; then
      BASE_URL="${value}"
      DOMAIN="${host}"
      return
    fi
    warn "public base URL must include a host"
  done
}

prompt_new_config_values() {
  prompt_base_url
  BIND="$(prompt_text "Local bind address" "${BIND}")"

  if prompt_yes_no "Disable Web admin at ${BASE_URL}/admin?" "y"; then
    ADMIN_ENABLED="false"
    ADMIN_USERS=""
  else
    ADMIN_ENABLED="true"
    ADMIN_USERS="gono"
  fi
}

configure_config_file() {
  local backup_path

  if [[ -f "${CONFIG_FILE}" ]]; then
    log "existing config found: ${CONFIG_FILE}"
    if prompt_yes_no "Keep existing config during this install/upgrade?" "y"; then
      if ! validate_existing_config_for_preserve; then
        if prompt_yes_no "Back up the existing config and write a fresh one?" "y"; then
          backup_path="${CONFIG_FILE}.bak.$(date +%Y%m%d%H%M%S)"
          cp -p "${CONFIG_FILE}" "${backup_path}"
          log "backed up existing config to ${backup_path}"
        else
          die "fix ${CONFIG_FILE} and rerun the installer"
        fi
      else
        PRESERVE_CONFIG="1"
        load_existing_config_defaults
        return
      fi
    fi
  fi

  PRESERVE_CONFIG="0"
  prompt_new_config_values
}

write_config() {
  if [[ "${PRESERVE_CONFIG}" == "1" ]]; then
    log "keeping existing config ${CONFIG_FILE}"
    return
  fi

  log "writing ${CONFIG_FILE}"
  cat >"${CONFIG_FILE}" <<EOF
[server]
bind = "$(toml_escape "${BIND}")"
cert_file = "$(toml_escape "${TLS_DIR}/cert.pem")"
key_file = "$(toml_escape "${TLS_DIR}/key.pem")"
base_url = "$(toml_escape "${BASE_URL}")"

[storage]
data_dir = "$(toml_escape "${DATA_DIR}")"
xattr_ns = "$(toml_escape "${XATTR_NS}")"

[db]
path = "$(toml_escape "${DB_PATH}")"
max_connections = ${MAX_CONNECTIONS}

[auth]
realm = "$(toml_escape "${AUTH_REALM}")"

[admin]
enabled = ${ADMIN_ENABLED}
users = $(toml_string_array_from_csv "${ADMIN_USERS}")

[sync]
change_log_retention_days = 30
change_log_min_entries = 10000

[notify_push]
enabled = true
path = "/push"
advertised_types = ["files", "activities", "notifications"]
pre_auth_ttl_secs = 15
user_connection_limit = 64
max_debounce_secs = 15
ping_interval_secs = 30
auth_timeout_secs = 15
max_connection_secs = 0
EOF
  chown root:"${RUN_GROUP}" "${CONFIG_FILE}"
  chmod 0640 "${CONFIG_FILE}"
}

write_systemd_unit() {
  local unit="/etc/systemd/system/${SERVICE_NAME}.service"
  log "writing ${unit}"
  cat >"${unit}" <<EOF
[Unit]
Description=Gono Cloud Gono Cloud compatible WebDAV service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
Group=${RUN_GROUP}
WorkingDirectory=${STATE_DIR}
Environment=GONE_CLOUD_CONFIG=${CONFIG_FILE}
Environment=GONE_CLOUD_INSECURE_HTTP=${INSECURE_HTTP}
Environment=GONE_CLOUD_LOG_FORMAT=${LOG_FORMAT}
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
        <key>GONE_CLOUD_CONFIG</key>
        <string>$(xml_escape "${CONFIG_FILE}")</string>
        <key>GONE_CLOUD_INSECURE_HTTP</key>
        <string>$(xml_escape "${INSECURE_HTTP}")</string>
        <key>GONE_CLOUD_LOG_FORMAT</key>
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
  local port="${BIND##*:}"
  echo "http://127.0.0.1:${port}/status.php"
}

show_service_logs_tail() {
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
  local _
  for _ in $(seq 1 60); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if ! service_is_active; then
      show_service_logs_tail
      die "${SERVICE_NAME} failed to start"
    fi
    sleep 1
  done

  show_service_logs_tail
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
  if ! systemctl restart "${SERVICE_NAME}"; then
    show_service_logs_tail
    die "failed to start systemd service ${SERVICE_NAME}"
  fi
}

prepare_macos_log_files() {
  touch "${LOG_DIR}/stdout.log" "${LOG_DIR}/stderr.log"
  chown "${RUN_USER}:${RUN_GROUP}" "${LOG_DIR}/stdout.log" "${LOG_DIR}/stderr.log"
  capture_macos_log_offsets
}

wait_for_macos_service_unloaded() {
  local target="$1"
  local _

  for _ in $(seq 1 20); do
    if ! launchctl print "${target}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done

  warn "launchd still reports ${target} as loaded after bootout"
}

bootstrap_macos_service() {
  local target="$1"
  local attempt output

  for attempt in 1 2 3; do
    if output="$(launchctl bootstrap system "${PLIST_PATH}" 2>&1)"; then
      return 0
    fi

    if launchctl print "${target}" >/dev/null 2>&1; then
      warn "launchctl bootstrap reported an error, but ${target} is loaded; continuing with kickstart"
      return 0
    fi

    warn "launchctl bootstrap failed on attempt ${attempt}: ${output}"
    sleep "${attempt}"
  done

  show_service_logs_tail
  die "failed to bootstrap launchd service from ${PLIST_PATH}"
}

start_macos_service() {
  local target="system/${SERVICE_NAME}"
  prepare_macos_log_files

  [[ -f "${PLIST_PATH}" ]] || die "launchd plist is not installed at ${PLIST_PATH}"
  launchctl bootout "${target}" >/dev/null 2>&1 \
    || launchctl bootout system "${PLIST_PATH}" >/dev/null 2>&1 \
    || true
  wait_for_macos_service_unloaded "${target}"
  bootstrap_macos_service "${target}"
  launchctl enable "${target}" >/dev/null 2>&1 || true
  if ! launchctl kickstart -k "${target}"; then
    show_service_logs_tail
    die "failed to kickstart launchd service ${target}"
  fi
}

restart_linux_service() {
  local unit="/etc/systemd/system/${SERVICE_NAME}.service"
  [[ -f "${unit}" ]] || die "systemd unit is not installed at ${unit}"
  systemctl daemon-reload
  systemctl reset-failed "${SERVICE_NAME}" >/dev/null 2>&1 || true
  if ! systemctl restart "${SERVICE_NAME}"; then
    show_service_logs_tail
    die "failed to restart systemd service ${SERVICE_NAME}"
  fi
}

restart_macos_service() {
  local target="system/${SERVICE_NAME}"
  prepare_macos_log_files

  [[ -f "${PLIST_PATH}" ]] || die "launchd plist is not installed at ${PLIST_PATH}"
  if launchctl print "${target}" >/dev/null 2>&1; then
    if ! launchctl kickstart -k "${target}"; then
      show_service_logs_tail
      die "failed to restart launchd service ${target}"
    fi
  else
    bootstrap_macos_service "${target}"
    launchctl enable "${target}" >/dev/null 2>&1 || true
    if ! launchctl kickstart -k "${target}"; then
      show_service_logs_tail
      die "failed to start launchd service ${target}"
    fi
  fi
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

show_service_status() {
  case "${PLATFORM}" in
    linux)
      require_cmd systemctl
      log "service: ${SERVICE_NAME}"
      log "binary: ${BIN_PATH}"
      log "config: ${CONFIG_FILE}"
      log "data: ${DATA_DIR}"
      log "logs: ${LOG_DIR}"
      local active enabled url
      active="$(systemctl is-active "${SERVICE_NAME}" 2>/dev/null || true)"
      enabled="$(systemctl is-enabled "${SERVICE_NAME}" 2>/dev/null || true)"
      log "systemd active: ${active:-unknown}"
      log "systemd enabled: ${enabled:-unknown}"
      url="$(health_url)"
      if command -v curl >/dev/null 2>&1 && curl -fsS "${url}" >/dev/null 2>&1; then
        log "health: ok (${url})"
      else
        warn "health: not reachable (${url})"
      fi
      if ! systemctl status "${SERVICE_NAME}" --no-pager -l; then
        warn "systemctl status exited non-zero; service may be inactive or not installed"
      fi
      ;;
    macos)
      require_cmd launchctl
      log "service: ${SERVICE_NAME}"
      log "binary: ${BIN_PATH}"
      log "config: ${CONFIG_FILE}"
      log "data: ${DATA_DIR}"
      log "logs: ${LOG_DIR}"
      local output state pid last_exit url
      url="$(health_url)"
      if command -v curl >/dev/null 2>&1 && curl -fsS "${url}" >/dev/null 2>&1; then
        log "health: ok (${url})"
      else
        warn "health: not reachable (${url})"
      fi
      if output="$(launchctl print "system/${SERVICE_NAME}" 2>&1)"; then
        state="$(printf '%s\n' "${output}" | sed -n 's/^[[:space:]]*state = //p' | head -n 1)"
        pid="$(printf '%s\n' "${output}" | sed -n 's/^[[:space:]]*pid = //p' | head -n 1)"
        last_exit="$(printf '%s\n' "${output}" | sed -n 's/^[[:space:]]*last exit code = //p' | head -n 1)"
        log "launchd state: ${state:-loaded}"
        if [[ -n "${pid}" ]]; then
          log "pid: ${pid}"
        fi
        if [[ -n "${last_exit}" ]]; then
          log "last exit code: ${last_exit}"
        fi
      else
        warn "launchctl status is unavailable for system/${SERVICE_NAME}"
        printf '%s\n' "${output}" >&2
        if [[ -f "${PLIST_PATH}" ]]; then
          log "plist exists: ${PLIST_PATH}"
        else
          warn "plist not found: ${PLIST_PATH}"
        fi
      fi
      ;;
  esac
}

follow_service_logs() {
  case "${PLATFORM}" in
    linux)
      require_cmd journalctl
      journalctl -u "${SERVICE_NAME}" -f --no-pager
      ;;
    macos)
      require_cmd tail
      local stdout_log stderr_log readable_logs
      stdout_log="${LOG_DIR}/stdout.log"
      stderr_log="${LOG_DIR}/stderr.log"
      readable_logs=()

      if [[ -r "${stdout_log}" ]]; then
        readable_logs+=("${stdout_log}")
      else
        warn "stdout log is not readable: ${stdout_log}"
      fi
      if [[ -r "${stderr_log}" ]]; then
        readable_logs+=("${stderr_log}")
      else
        warn "stderr log is not readable: ${stderr_log}"
      fi

      if [[ "${#readable_logs[@]}" -eq 0 ]]; then
        die "no readable macOS log files found. Check '$(service_status_hint)'."
      fi
      tail -f "${readable_logs[@]}"
      ;;
  esac
}

restart_existing_service() {
  local url

  require_cmd curl
  case "${PLATFORM}" in
    linux)
      require_cmd systemctl
      restart_linux_service
      ;;
    macos)
      require_cmd launchctl
      restart_macos_service
      ;;
  esac

  url="$(health_url)"
  wait_for_service "${url}"
  log "restarted ${SERVICE_NAME}"
  log "local health: ${url}"
}

stop_existing_service() {
  case "${PLATFORM}" in
    linux)
      if command -v systemctl >/dev/null 2>&1; then
        systemctl disable --now "${SERVICE_NAME}" >/dev/null 2>&1 || true
        systemctl daemon-reload >/dev/null 2>&1 || true
      fi
      ;;
    macos)
      if command -v launchctl >/dev/null 2>&1; then
        launchctl bootout system "${PLIST_PATH}" >/dev/null 2>&1 || true
      fi
      ;;
  esac
}

confirm_uninstall() {
  warn "uninstall will stop ${SERVICE_NAME} and remove service files plus ${BIN_PATH}"
  warn "config, database, stored files, and logs are preserved by default"
  if ! prompt_yes_no "Continue uninstall and keep data?" "y"; then
    log "uninstall cancelled"
    exit 0
  fi

  if prompt_yes_no "Don't remove config, data, and logs?" "y"; then
    PURGE="0"
  else
    PURGE="1"
  fi
}

uninstall_service() {
  confirm_uninstall
  stop_existing_service

  case "${PLATFORM}" in
    linux)
      rm -f "/etc/systemd/system/${SERVICE_NAME}.service"
      if command -v systemctl >/dev/null 2>&1; then
        systemctl daemon-reload >/dev/null 2>&1 || true
      fi
      ;;
    macos)
      rm -f "${PLIST_PATH}"
      ;;
  esac

  rm -f "${BIN_PATH}"
  rmdir "${BIN_DIR}" "${INSTALL_DIR}" >/dev/null 2>&1 || true

  if [[ "${PURGE}" == "1" ]]; then
    rm -rf "${CONFIG_DIR}" "${STATE_DIR}" "${LOG_DIR}"
  fi

  log "uninstalled ${APP_NAME}"
  if [[ "${PURGE}" != "1" ]]; then
    log "preserved config: ${CONFIG_DIR}"
    log "preserved state: ${STATE_DIR}"
    log "preserved logs: ${LOG_DIR}"
  fi
}

install_service() {
  local start_time url password
  log "installing for ${PLATFORM}"
  install_packages
  require_platform_commands

  ensure_run_identity
  prepare_directories
  configure_config_file
  install_binary
  write_config
  write_service_definition

  if [[ "${INSECURE_HTTP}" == "1" && "${BIND}" != 127.* && "${BIND}" != localhost:* ]]; then
    warn "GONE_CLOUD_INSECURE_HTTP=1 with non-loopback bind '${BIND}'. Put this behind trusted network controls or enable TLS."
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
  warn "Important: ${APP_NAME} listens only on the local machine interface(${BIND}) by default. Use Nginx or another reverse proxy to expose ${BASE_URL} before accessing it from other machines."
  if [[ "${ADMIN_ENABLED}" == "true" ]]; then
    log "admin URL: ${BASE_URL}/admin"
  fi
  if [[ -n "${password}" ]]; then
    log "bootstrap user: gono"
    log "bootstrap app password: ${password}"
    warn "save this password now; normal restarts will not print it again"
  else
    log "no new bootstrap password was printed; existing database/password was preserved"
  fi
  log "reverse proxy target: http://${BIND} (include WebSocket upgrade for ${BASE_URL}/push/ws)"
}

dispatch_action() {
  local action="$1"

  case "${action}" in
    install)
      install_service
      ;;
    restart)
      restart_existing_service
      ;;
    uninstall)
      uninstall_service
      ;;
    status)
      show_service_status
      ;;
    logs)
      follow_service_logs
      ;;
    *)
      die "unknown internal action: ${action}"
      ;;
  esac
}

main() {
  require_cmd uname
  resolve_script_context

  if [[ "$#" -gt 0 ]]; then
    reject_args
  fi

  if [[ -n "${INTERNAL_LOCAL_BIN}" ]]; then
    LOCAL_BIN="${INTERNAL_LOCAL_BIN}"
  fi

  set_platform_defaults

  if [[ -n "${INTERNAL_ACTION}" ]]; then
    [[ "${EUID}" -eq 0 ]] || die "internal action requires root"
    dispatch_action "${INTERNAL_ACTION}"
    return
  fi

  show_interactive_menu
}

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

main "$@"
