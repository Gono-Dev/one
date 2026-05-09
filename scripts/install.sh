#!/usr/bin/env bash
set -euo pipefail

APP_NAME="gono-cloud"
INSTALL_URL="${GONO_CLOUD_INSTALL_URL:-https://run.gono.cloud}"
INSTALL_SOURCE="${GONO_CLOUD_INSTALL_SOURCE:-auto}"
LOCAL_BUILD="${GONO_CLOUD_LOCAL_BUILD:-auto}"
LOCAL_BUILD_PROFILE="${GONO_CLOUD_BUILD_PROFILE:-release}"
LOCAL_BIN="${GONO_CLOUD_BIN:-${GONO_CLOUD_LOCAL_BIN:-}}"
COMMAND="${GONO_CLOUD_COMMAND:-}"
ASSUME_YES="${GONO_CLOUD_YES:-0}"
PURGE="${GONO_CLOUD_PURGE:-0}"

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
DOMAIN="${GONO_CLOUD_DOMAIN:-gono.cloud}"
BASE_URL="${GONO_CLOUD_BASE_URL:-https://${DOMAIN}}"
BASE_URL_EXPLICIT=0
if [[ -n "${GONO_CLOUD_BASE_URL+x}" ]]; then
  BASE_URL_EXPLICIT=1
fi
BIND="${GONO_CLOUD_BIND:-127.0.0.1:16102}"
XATTR_NS="${GONO_CLOUD_XATTR_NS:-user.nc}"
AUTH_REALM="${GONO_CLOUD_AUTH_REALM:-Nextcloud}"
MAX_CONNECTIONS="${GONO_CLOUD_DB_MAX_CONNECTIONS:-5}"
LOG_FORMAT="${GONO_CLOUD_LOG_FORMAT:-text}"
RUST_LOG_VALUE="${RUST_LOG:-info}"
INSECURE_HTTP="${GONO_CLOUD_INSECURE_HTTP:-1}"
RELEASE_REPO="${GONO_CLOUD_RELEASE_REPO:-Gono-Dev/cloud.server}"
RELEASE_BASE="${GONO_CLOUD_RELEASE_BASE:-https://github.com/${RELEASE_REPO}/releases}"
VERSION="${GONO_CLOUD_VERSION:-latest}"
BIN_URL="${GONO_CLOUD_BIN_URL:-}"
HEALTH_URL="${GONO_CLOUD_HEALTH_URL:-}"

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

show_usage() {
  cat <<EOF
Gono Cloud installer

Usage:
  $(usage_name)
  $(usage_name) install [options]
  $(usage_name) status [options]
  $(usage_name) logs [options]
  $(usage_name) restart [options]
  $(usage_name) users [options]
  $(usage_name) uninstall [options]
  bash <(curl -sL ${INSTALL_URL})
  bash <(curl -sL ${INSTALL_URL}) install [options]

Commands:
  no arguments              Start the interactive menu
  install                   Install or upgrade Gono Cloud
  status                    Show service status
  logs, show_log            Follow service logs
  restart                   Restart service and run health check
  users                     Manage local application users
  uninstall                 Stop service and remove service files/binary
  help                      Show this help

Source options:
  --local                    Use local repository/binary source
  --release                  Download a release artifact
  --install-source VALUE     auto, local, or release (default: ${INSTALL_SOURCE})
  --bin PATH                 Install this local gono-cloud binary
  --bin-url URL              Download this exact binary/archive URL
  --version VERSION          Release version or latest (default: ${VERSION})
  --release-base URL         GitHub releases base URL (default: ${RELEASE_BASE})
  --sha256 HASH              Verify downloaded artifact sha256

Local build options:
  --build-profile VALUE      release or debug (default: ${LOCAL_BUILD_PROFILE})
  --debug                    Same as --build-profile debug
  --local-build              Force cargo build for local source
  --no-local-build           Reuse existing local binary

Target options:
  --arch ARCH                Override architecture detection
  --domain DOMAIN            Public domain (default: ${DOMAIN})
  --base-url URL             Public base URL (default: ${BASE_URL})
  --bind ADDR                Local bind address (default: ${BIND})
  --install-dir DIR          Install prefix (default: platform-specific)
  --config FILE              Config file path
  --config-dir DIR           Config directory
  --state-dir DIR            State directory
  --data-dir DIR             Data directory
  --db-path FILE             SQLite database path
  --log-dir DIR              Log directory
  --tls-dir DIR              TLS directory

Service options:
  --service-name NAME        systemd/launchd service name
  --user USER                Service user
  --group GROUP              Service group
  --health-url URL           Health check URL
  --insecure-http VALUE      1 or 0 (default: ${INSECURE_HTTP})
  --no-insecure-http         Same as --insecure-http 0
  --log-format VALUE         text, compact, or json (default: ${LOG_FORMAT})
  --rust-log VALUE           RUST_LOG value (default: ${RUST_LOG_VALUE})

Other:
  -h, --help, help           Show this help
  -y, --yes                  Skip confirmation prompts
  --purge                    With uninstall, also remove config, data, and logs

Examples:
  scripts/install.sh
  scripts/install.sh install --debug
  scripts/install.sh install --release --version latest
  scripts/install.sh status
  scripts/install.sh logs
  scripts/install.sh restart
  scripts/install.sh users
  scripts/install.sh uninstall
  scripts/install.sh uninstall --purge
  scripts/install.sh install --bin target/release/gono-cloud
  scripts/install.sh install --release --version latest
  scripts/install.sh install --domain files.example.com
  bash <(curl -sL ${INSTALL_URL}) install --base-url https://files.example.com

Environment variables are still supported. For example:
  GONO_CLOUD_BIN=/path/to/gono-cloud scripts/install.sh install
  GONO_CLOUD_INSTALL_SOURCE=release scripts/install.sh install
EOF
}

set_command() {
  COMMAND="$1"
  export GONO_CLOUD_COMMAND="${COMMAND}"
}

is_enabled() {
  case "$1" in
    1|true|yes|y|on)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

prompt_uninstall_purge() {
  local input
  if is_enabled "${ASSUME_YES}"; then
    return
  fi

  printf "Remove config, data, and logs too? [y/N] "
  if ! read -r input; then
    die "failed to read purge selection"
  fi
  case "${input}" in
    [yY]|[yY][eE][sS])
      PURGE="1"
      export GONO_CLOUD_PURGE="${PURGE}"
      ;;
  esac
}

show_interactive_menu() {
  local choice

  if [[ ! -t 0 ]]; then
    warn "no interactive terminal detected; reading menu selection from stdin"
  fi

  while true; do
    cat <<EOF
Gono Cloud installer

Please select an action:
  1. Install or upgrade Gono Cloud
  2. Uninstall Gono Cloud
  3. Restart service
  4. Show service status
  5. Show service logs
  6. User management
  7. Help
  0. Exit
EOF
    printf "Enter choice [1-7,0]: "
    if ! read -r choice; then
      die "failed to read menu selection. For non-interactive use, run '$(usage_name) install' or '$(usage_name) uninstall'."
    fi

    case "${choice}" in
      1|install|i|I)
        set_command "install"
        return
        ;;
      2|uninstall|remove|u|U)
        set_command "uninstall"
        prompt_uninstall_purge
        return
        ;;
      3|restart|r|R)
        set_command "restart"
        return
        ;;
      4|status|s|S)
        set_command "status"
        return
        ;;
      5|logs|log|l|L)
        set_command "logs"
        return
        ;;
      6|users|user|u|U)
        set_command "users"
        return
        ;;
      7|help|h|H|\?)
        show_usage
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

default_command_if_missing() {
  if [[ -z "${COMMAND}" ]]; then
    set_command "install"
  fi
}

need_arg() {
  local option="$1"
  local value="${2:-}"
  [[ -n "${value}" ]] || die "${option} requires a value"
}

parse_args() {
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      -h|--help|help)
        show_usage
        exit 0
        ;;
      install)
        set_command "install"
        shift
        ;;
      status|show_status)
        set_command "status"
        shift
        ;;
      logs|log|show_log)
        set_command "logs"
        shift
        ;;
      restart|restart_and_update)
        set_command "restart"
        shift
        ;;
      users|user|user-management|manage-users)
        set_command "users"
        shift
        ;;
      uninstall|remove)
        set_command "uninstall"
        shift
        ;;
      --local)
        INSTALL_SOURCE="local"
        export GONO_CLOUD_INSTALL_SOURCE="${INSTALL_SOURCE}"
        shift
        ;;
      --release)
        INSTALL_SOURCE="release"
        export GONO_CLOUD_INSTALL_SOURCE="${INSTALL_SOURCE}"
        shift
        ;;
      --install-source)
        need_arg "$1" "${2:-}"
        INSTALL_SOURCE="$2"
        export GONO_CLOUD_INSTALL_SOURCE="${INSTALL_SOURCE}"
        shift 2
        ;;
      --bin)
        need_arg "$1" "${2:-}"
        LOCAL_BIN="$2"
        export GONO_CLOUD_BIN="${LOCAL_BIN}"
        shift 2
        ;;
      --bin-url)
        need_arg "$1" "${2:-}"
        BIN_URL="$2"
        export GONO_CLOUD_BIN_URL="${BIN_URL}"
        shift 2
        ;;
      --version)
        need_arg "$1" "${2:-}"
        VERSION="$2"
        export GONO_CLOUD_VERSION="${VERSION}"
        shift 2
        ;;
      --release-base)
        need_arg "$1" "${2:-}"
        RELEASE_BASE="$2"
        export GONO_CLOUD_RELEASE_BASE="${RELEASE_BASE}"
        shift 2
        ;;
      --sha256)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_SHA256="$2"
        shift 2
        ;;
      --build-profile)
        need_arg "$1" "${2:-}"
        LOCAL_BUILD_PROFILE="$2"
        export GONO_CLOUD_BUILD_PROFILE="${LOCAL_BUILD_PROFILE}"
        shift 2
        ;;
      --debug)
        LOCAL_BUILD_PROFILE="debug"
        export GONO_CLOUD_BUILD_PROFILE="${LOCAL_BUILD_PROFILE}"
        shift
        ;;
      --local-build)
        LOCAL_BUILD="1"
        export GONO_CLOUD_LOCAL_BUILD="${LOCAL_BUILD}"
        shift
        ;;
      --no-local-build)
        LOCAL_BUILD="0"
        export GONO_CLOUD_LOCAL_BUILD="${LOCAL_BUILD}"
        shift
        ;;
      --arch)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_ARCH="$2"
        shift 2
        ;;
      --domain)
        need_arg "$1" "${2:-}"
        DOMAIN="$2"
        export GONO_CLOUD_DOMAIN="${DOMAIN}"
        if [[ "${BASE_URL_EXPLICIT}" == "0" ]]; then
          BASE_URL="https://${DOMAIN}"
          export GONO_CLOUD_BASE_URL="${BASE_URL}"
        fi
        shift 2
        ;;
      --base-url)
        need_arg "$1" "${2:-}"
        BASE_URL="$2"
        BASE_URL_EXPLICIT=1
        export GONO_CLOUD_BASE_URL="${BASE_URL}"
        shift 2
        ;;
      --bind)
        need_arg "$1" "${2:-}"
        BIND="$2"
        export GONO_CLOUD_BIND="${BIND}"
        shift 2
        ;;
      --install-dir)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_INSTALL_DIR="$2"
        shift 2
        ;;
      --config)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_CONFIG="$2"
        shift 2
        ;;
      --config-dir)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_CONFIG_DIR="$2"
        shift 2
        ;;
      --state-dir)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_STATE_DIR="$2"
        shift 2
        ;;
      --data-dir)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_DATA_DIR="$2"
        shift 2
        ;;
      --db-path)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_DB_PATH="$2"
        shift 2
        ;;
      --log-dir)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_LOG_DIR="$2"
        shift 2
        ;;
      --tls-dir)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_TLS_DIR="$2"
        shift 2
        ;;
      --service-name)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_SERVICE_NAME="$2"
        shift 2
        ;;
      --user)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_USER="$2"
        shift 2
        ;;
      --group)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_GROUP="$2"
        shift 2
        ;;
      --health-url)
        need_arg "$1" "${2:-}"
        HEALTH_URL="$2"
        export GONO_CLOUD_HEALTH_URL="${HEALTH_URL}"
        shift 2
        ;;
      --insecure-http)
        need_arg "$1" "${2:-}"
        INSECURE_HTTP="$2"
        export GONO_CLOUD_INSECURE_HTTP="${INSECURE_HTTP}"
        shift 2
        ;;
      --no-insecure-http)
        INSECURE_HTTP="0"
        export GONO_CLOUD_INSECURE_HTTP="${INSECURE_HTTP}"
        shift
        ;;
      --log-format)
        need_arg "$1" "${2:-}"
        LOG_FORMAT="$2"
        export GONO_CLOUD_LOG_FORMAT="${LOG_FORMAT}"
        shift 2
        ;;
      --rust-log)
        need_arg "$1" "${2:-}"
        RUST_LOG_VALUE="$2"
        export RUST_LOG="${RUST_LOG_VALUE}"
        shift 2
        ;;
      --xattr-ns)
        need_arg "$1" "${2:-}"
        XATTR_NS="$2"
        export GONO_CLOUD_XATTR_NS="${XATTR_NS}"
        shift 2
        ;;
      --auth-realm)
        need_arg "$1" "${2:-}"
        AUTH_REALM="$2"
        export GONO_CLOUD_AUTH_REALM="${AUTH_REALM}"
        shift 2
        ;;
      --max-connections)
        need_arg "$1" "${2:-}"
        MAX_CONNECTIONS="$2"
        export GONO_CLOUD_DB_MAX_CONNECTIONS="${MAX_CONNECTIONS}"
        shift 2
        ;;
      --plist-path)
        need_arg "$1" "${2:-}"
        export GONO_CLOUD_PLIST_PATH="$2"
        shift 2
        ;;
      -y|--yes)
        ASSUME_YES="1"
        export GONO_CLOUD_YES="${ASSUME_YES}"
        shift
        ;;
      --purge)
        PURGE="1"
        export GONO_CLOUD_PURGE="${PURGE}"
        shift
        ;;
      --)
        shift
        [[ "$#" -eq 0 ]] || die "unexpected extra arguments: $*"
        ;;
      -*)
        show_usage >&2
        die "unknown option: $1"
        ;;
      *)
        show_usage >&2
        die "unknown argument: $1"
        ;;
    esac
  done
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
      SERVICE_NAME="${GONO_CLOUD_SERVICE_NAME:-gono-cloud}"
      INSTALL_DIR="${GONO_CLOUD_INSTALL_DIR:-/opt/gono-cloud}"
      CONFIG_DIR="${GONO_CLOUD_CONFIG_DIR:-/etc/gono-cloud}"
      STATE_DIR="${GONO_CLOUD_STATE_DIR:-/var/lib/gono-cloud}"
      LOG_DIR="${GONO_CLOUD_LOG_DIR:-/var/log/gono-cloud}"
      RUN_USER="${GONO_CLOUD_USER:-gono-cloud}"
      RUN_GROUP="${GONO_CLOUD_GROUP:-gono-cloud}"
      ;;
    macos)
      PACKAGE_MANAGER="none"
      SERVICE_NAME="${GONO_CLOUD_SERVICE_NAME:-cloud.gono.gono-cloud}"
      INSTALL_DIR="${GONO_CLOUD_INSTALL_DIR:-/opt/gono-cloud}"
      CONFIG_DIR="${GONO_CLOUD_CONFIG_DIR:-/Library/Application Support/Gono Cloud}"
      STATE_DIR="${GONO_CLOUD_STATE_DIR:-/Library/Application Support/Gono Cloud}"
      LOG_DIR="${GONO_CLOUD_LOG_DIR:-/Library/Logs/Gono Cloud}"
      RUN_USER="${GONO_CLOUD_USER:-root}"
      RUN_GROUP="${GONO_CLOUD_GROUP:-wheel}"
      ;;
    *)
      die "unsupported platform: ${PLATFORM}"
      ;;
  esac

  BIN_DIR="${INSTALL_DIR}/bin"
  BIN_PATH="${GONO_CLOUD_BIN_PATH:-${BIN_DIR}/${APP_NAME}}"
  CONFIG_FILE="${GONO_CLOUD_CONFIG:-${CONFIG_DIR}/config.toml}"
  DATA_DIR="${GONO_CLOUD_DATA_DIR:-${STATE_DIR}/data}"
  DB_PATH="${GONO_CLOUD_DB_PATH:-${STATE_DIR}/gono-cloud.db}"
  TLS_DIR="${GONO_CLOUD_TLS_DIR:-${CONFIG_DIR}/tls}"
  PLIST_PATH="${GONO_CLOUD_PLIST_PATH:-/Library/LaunchDaemons/${SERVICE_NAME}.plist}"
}

require_root() {
  local -a forwarded_args
  forwarded_args=("$@")

  if [[ "${EUID}" -eq 0 ]]; then
    return
  fi

  if [[ "$#" -eq 0 && -n "${COMMAND}" ]]; then
    forwarded_args=("${COMMAND}")
    if is_enabled "${PURGE}"; then
      forwarded_args+=("--purge")
    fi
    if is_enabled "${ASSUME_YES}"; then
      forwarded_args+=("--yes")
    fi
  fi

  if command -v sudo >/dev/null 2>&1; then
    log "re-running installer with sudo"
    if [[ -n "${SCRIPT_PATH}" && -r "${SCRIPT_PATH}" ]]; then
      sudo -E bash "${SCRIPT_PATH}" "${forwarded_args[@]}"
    else
      curl -fsSL "${INSTALL_URL}" | sudo -E bash -s -- "${forwarded_args[@]}"
    fi
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
  machine="${GONO_CLOUD_ARCH:-$(uname -m)}"

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

release_tag() {
  local version="$1"
  case "${version}" in
    v*)
      echo "${version}"
      ;;
    *)
      echo "v${version}"
      ;;
  esac
}

release_asset_version() {
  local version="$1"
  echo "${version#v}"
}

artifact_url() {
  local arch os target tag asset_version
  arch="$(target_arch)"
  os="$(target_os)"
  target="${os}-${arch}"

  if [[ -n "${BIN_URL}" ]]; then
    echo "${BIN_URL}"
  elif [[ "${VERSION}" == "latest" ]]; then
    echo "${RELEASE_BASE}/latest/download/${APP_NAME}-${target}.tar.gz"
  else
    tag="$(release_tag "${VERSION}")"
    asset_version="$(release_asset_version "${VERSION}")"
    echo "${RELEASE_BASE}/download/${tag}/${APP_NAME}-${asset_version}-${target}.tar.gz"
  fi
}

use_local_source() {
  case "${INSTALL_SOURCE}" in
    local)
      [[ -n "${LOCAL_BIN}" || -n "${REPO_ROOT}" ]] \
        || die "GONO_CLOUD_INSTALL_SOURCE=local requires a local repo or GONO_CLOUD_BIN"
      return 0
      ;;
    release)
      return 1
      ;;
    auto)
      [[ -z "${BIN_URL}" && -n "${REPO_ROOT}" ]]
      ;;
    *)
      die "unsupported GONO_CLOUD_INSTALL_SOURCE='${INSTALL_SOURCE}'. Use auto, local, or release."
      ;;
  esac
}

target_dir() {
  if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
    case "${CARGO_TARGET_DIR}" in
      /*)
        printf '%s\n' "${CARGO_TARGET_DIR}"
        ;;
      *)
        printf '%s/%s\n' "$(pwd -P)" "${CARGO_TARGET_DIR}"
        ;;
    esac
  else
    printf '%s\n' "${REPO_ROOT}/target"
  fi
}

local_binary_candidate() {
  local profile_dir
  case "${LOCAL_BUILD_PROFILE}" in
    release)
      profile_dir="release"
      ;;
    debug|dev)
      profile_dir="debug"
      ;;
    *)
      die "unsupported GONO_CLOUD_BUILD_PROFILE='${LOCAL_BUILD_PROFILE}'. Use release or debug."
      ;;
  esac

  printf '%s/%s/%s\n' "$(target_dir)" "${profile_dir}" "${APP_NAME}"
}

should_build_local_binary() {
  case "${LOCAL_BUILD}" in
    1|true|yes)
      return 0
      ;;
    0|false|no)
      return 1
      ;;
    auto)
      command -v cargo >/dev/null 2>&1
      ;;
    *)
      die "unsupported GONO_CLOUD_LOCAL_BUILD='${LOCAL_BUILD}'. Use auto, 1, or 0."
      ;;
  esac
}

build_local_binary() {
  [[ -n "${REPO_ROOT}" ]] || die "cannot build local binary: repository root was not detected"
  require_cmd cargo

  case "${LOCAL_BUILD_PROFILE}" in
    release)
      log "building local release binary from ${REPO_ROOT}" >&2
      cargo build --locked --release --manifest-path "${REPO_ROOT}/Cargo.toml"
      ;;
    debug|dev)
      log "building local debug binary from ${REPO_ROOT}" >&2
      cargo build --locked --manifest-path "${REPO_ROOT}/Cargo.toml"
      ;;
    *)
      die "unsupported GONO_CLOUD_BUILD_PROFILE='${LOCAL_BUILD_PROFILE}'. Use release or debug."
      ;;
  esac
}

normalize_path() {
  local path="$1"
  case "${path}" in
    /*)
      printf '%s\n' "${path}"
      ;;
    *)
      printf '%s/%s\n' "$(pwd -P)" "${path}"
      ;;
  esac
}

resolve_local_binary() {
  local candidate
  if [[ -n "${LOCAL_BIN}" ]]; then
    candidate="$(normalize_path "${LOCAL_BIN}")"
  else
    candidate="$(local_binary_candidate)"
    if should_build_local_binary; then
      build_local_binary
    fi
  fi

  [[ -x "${candidate}" ]] || die "local binary is not executable: ${candidate}. Set GONO_CLOUD_BIN or allow GONO_CLOUD_LOCAL_BUILD=auto."
  printf '%s\n' "${candidate}"
}

prepare_local_binary_before_sudo() {
  if [[ "${EUID}" -eq 0 ]]; then
    return
  fi
  if ! use_local_source; then
    return
  fi
  if [[ -n "${LOCAL_BIN}" ]]; then
    LOCAL_BIN="$(normalize_path "${LOCAL_BIN}")"
  else
    if should_build_local_binary; then
      build_local_binary
      LOCAL_BIN="$(local_binary_candidate)"
    else
      LOCAL_BIN="$(local_binary_candidate)"
    fi
  fi

  [[ -x "${LOCAL_BIN}" ]] || die "local binary is not executable: ${LOCAL_BIN}"
  export GONO_CLOUD_BIN="${LOCAL_BIN}"
  export GONO_CLOUD_INSTALL_SOURCE="local"
  export GONO_CLOUD_LOCAL_BUILD="0"
  INSTALL_SOURCE="local"
  LOCAL_BUILD="0"
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
    die "GONO_CLOUD_SHA256 was set but neither sha256sum nor shasum is available"
  fi
}

sha256_from_sidecar() {
  local file="$1"
  sed -n '1s/^\([0-9a-fA-F]\{64\}\).*/\1/p' "${file}"
}

verify_downloaded_artifact() {
  local url="$1"
  local artifact="$2"
  local sidecar sidecar_url expected

  if [[ -n "${GONO_CLOUD_SHA256:-}" ]]; then
    verify_sha256 "${GONO_CLOUD_SHA256}" "${artifact}"
    return
  fi

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
    die "failed to download release artifact from ${url}. Check the GitHub Release assets or set GONO_CLOUD_BIN_URL"
  fi

  verify_downloaded_artifact "${url}" "${artifact}"

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

install_local_binary() {
  local source_binary
  source_binary="$(resolve_local_binary)"
  log "installing local binary ${source_binary}"
  install -m 0755 "${source_binary}" "${BIN_PATH}"
}

install_binary() {
  if use_local_source; then
    install_local_binary
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
    die "macOS run user '${RUN_USER}' does not exist. Use GONO_CLOUD_USER=root or create the user first."
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
  fi
}

write_systemd_unit() {
  local unit="/etc/systemd/system/${SERVICE_NAME}.service"
  log "writing ${unit}"
  cat >"${unit}" <<EOF
[Unit]
Description=Gono Cloud Nextcloud-compatible WebDAV service
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
  if ! systemctl restart "${SERVICE_NAME}"; then
    show_service_logs
    die "failed to start systemd service ${SERVICE_NAME}"
  fi
}

prepare_macos_log_files() {
  touch "${LOG_DIR}/stdout.log" "${LOG_DIR}/stderr.log"
  chown "${RUN_USER}:${RUN_GROUP}" "${LOG_DIR}/stdout.log" "${LOG_DIR}/stderr.log"
  capture_macos_log_offsets
}

start_macos_service() {
  local target="system/${SERVICE_NAME}"
  prepare_macos_log_files

  [[ -f "${PLIST_PATH}" ]] || die "launchd plist is not installed at ${PLIST_PATH}"
  launchctl bootout "${target}" >/dev/null 2>&1 \
    || launchctl bootout system "${PLIST_PATH}" >/dev/null 2>&1 \
    || true
  if ! launchctl bootstrap system "${PLIST_PATH}"; then
    if launchctl print "${target}" >/dev/null 2>&1; then
      warn "launchctl bootstrap failed because ${target} is already loaded; using kickstart"
    else
      show_service_logs
      die "failed to bootstrap launchd service from ${PLIST_PATH}"
    fi
  fi
  launchctl enable "${target}" >/dev/null 2>&1 || true
  if ! launchctl kickstart -k "${target}"; then
    show_service_logs
    die "failed to kickstart launchd service ${target}"
  fi
}

restart_linux_service() {
  local unit="/etc/systemd/system/${SERVICE_NAME}.service"
  [[ -f "${unit}" ]] || die "systemd unit is not installed at ${unit}"
  systemctl daemon-reload
  systemctl reset-failed "${SERVICE_NAME}" >/dev/null 2>&1 || true
  if ! systemctl restart "${SERVICE_NAME}"; then
    show_service_logs
    die "failed to restart systemd service ${SERVICE_NAME}"
  fi
}

restart_macos_service() {
  local target="system/${SERVICE_NAME}"
  prepare_macos_log_files

  [[ -f "${PLIST_PATH}" ]] || die "launchd plist is not installed at ${PLIST_PATH}"
  if launchctl print "${target}" >/dev/null 2>&1; then
    if ! launchctl kickstart -k "${target}"; then
      show_service_logs
      die "failed to restart launchd service ${target}"
    fi
  else
    if ! launchctl bootstrap system "${PLIST_PATH}"; then
      show_service_logs
      die "failed to bootstrap launchd service from ${PLIST_PATH}"
    fi
    launchctl enable "${target}" >/dev/null 2>&1 || true
    if ! launchctl kickstart -k "${target}"; then
      show_service_logs
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
        die "no readable macOS log files found. Try 'sudo $(usage_name) logs' or check '$(service_status_hint)'."
      fi
      tail -f "${readable_logs[@]}"
      ;;
  esac
}

ensure_admin_binary() {
  [[ -x "${BIN_PATH}" ]] || die "Gono Cloud binary is not installed at ${BIN_PATH}. Install first, or set GONO_CLOUD_INSTALL_DIR/GONO_CLOUD_BIN_PATH."
}

run_user_cli() {
  ensure_admin_binary
  NC_DAV_CONFIG="${CONFIG_FILE}" "${BIN_PATH}" "$@"
}

prompt_local_user_name() {
  local prompt="$1"
  local username

  printf "%s" "${prompt}" >&2
  if ! read -r username; then
    die "failed to read username"
  fi
  username="${username//[[:space:]]/}"
  [[ -n "${username}" ]] || die "username cannot be empty"
  printf '%s\n' "${username}"
}

prompt_display_name() {
  local display_name
  printf "Display name (optional): " >&2
  if ! read -r display_name; then
    die "failed to read display name"
  fi
  printf '%s\n' "${display_name}"
}

confirm_delete_local_user() {
  local username="$1"
  local input

  if is_enabled "${ASSUME_YES}"; then
    return
  fi

  warn "this deletes login credentials for local user '${username}'. Stored files are not removed."
  printf "Delete user '${username}'? [y/N] "
  if ! read -r input; then
    die "failed to read delete confirmation"
  fi
  case "${input}" in
    [yY]|[yY][eE][sS])
      ;;
    *)
      log "user delete cancelled"
      return 1
      ;;
  esac
}

show_user_management_menu() {
  local choice username display_name

  while true; do
    cat <<EOF
Gono Cloud user management

Please select an action:
  1. List local users
  2. Add local user
  3. Delete local user
  0. Back
EOF
    printf "Enter choice [1-3,0]: "
    if ! read -r choice; then
      die "failed to read user management selection"
    fi

    case "${choice}" in
      1|list|l|L)
        run_user_cli user-list
        ;;
      2|add|a|A)
        username="$(prompt_local_user_name "Username: ")"
        display_name="$(prompt_display_name)"
        if [[ -n "${display_name}" ]]; then
          run_user_cli user-add "${username}" "${display_name}"
        else
          run_user_cli user-add "${username}"
        fi
        ;;
      3|delete|remove|d|D)
        username="$(prompt_local_user_name "Username to delete: ")"
        if confirm_delete_local_user "${username}"; then
          run_user_cli user-delete "${username}"
        fi
        ;;
      0|back|b|B|exit|quit|q|Q)
        return
        ;;
      *)
        warn "invalid selection: ${choice:-<empty>}"
        ;;
    esac
    printf '\n'
  done
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
  local input
  if [[ "${ASSUME_YES}" == "1" || "${ASSUME_YES}" == "true" || "${ASSUME_YES}" == "yes" ]]; then
    return
  fi

  warn "uninstall will stop ${SERVICE_NAME} and remove service files plus ${BIN_PATH}"
  if [[ "${PURGE}" == "1" || "${PURGE}" == "true" || "${PURGE}" == "yes" ]]; then
    warn "purge is enabled; config, data, and logs will also be removed"
  else
    warn "config, data, and logs will be preserved; pass --purge to remove them"
  fi
  printf "Continue? [y/N] "
  read -r input
  case "${input}" in
    [yY]|[yY][eE][sS])
      ;;
    *)
      log "uninstall cancelled"
      exit 0
      ;;
  esac
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

  if [[ "${PURGE}" == "1" || "${PURGE}" == "true" || "${PURGE}" == "yes" ]]; then
    rm -rf "${CONFIG_DIR}" "${STATE_DIR}" "${LOG_DIR}"
  fi

  log "uninstalled ${APP_NAME}"
  if [[ "${PURGE}" != "1" && "${PURGE}" != "true" && "${PURGE}" != "yes" ]]; then
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
  install_binary
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
  log "configure HTTPS reverse proxy, including WebSocket upgrade for ${BASE_URL}/push/ws, to forward to http://${BIND}"
}

dispatch_command() {
  case "${COMMAND}" in
    install)
      prepare_local_binary_before_sudo
      require_root "$@"
      install_service
      ;;
    status)
      show_service_status
      ;;
    logs)
      follow_service_logs
      ;;
    restart)
      require_root "$@"
      restart_existing_service
      ;;
    users)
      require_root "$@"
      show_user_management_menu
      ;;
    uninstall)
      require_root "$@"
      uninstall_service
      ;;
    *)
      show_usage >&2
      die "unknown command: ${COMMAND}"
      ;;
  esac
}

main() {
  require_cmd uname
  resolve_script_context
  if [[ "$#" -eq 0 && -z "${COMMAND}" ]]; then
    show_interactive_menu
  else
    parse_args "$@"
    default_command_if_missing
  fi
  set_platform_defaults
  dispatch_command "$@"
}

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

main "$@"
