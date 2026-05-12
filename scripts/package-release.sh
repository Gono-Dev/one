#!/usr/bin/env bash
set -euo pipefail

APP_NAME="gono-cloud"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${GONO_CLOUD_RELEASE_DIR:-${ROOT}/dist}"
SKIP_BUILD="${GONO_CLOUD_SKIP_BUILD:-0}"
CARGO_TARGET="${GONO_CLOUD_CARGO_TARGET:-}"
RELEASE_TARGET="${GONO_CLOUD_RELEASE_TARGET:-}"
BIN_PATH="${GONO_CLOUD_BIN:-}"

log() {
  printf '[gono-cloud-release] %s\n' "$*"
}

die() {
  printf '[gono-cloud-release] error: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

target_os() {
  case "$(uname -s)" in
    Linux)
      echo "linux"
      ;;
    Darwin)
      echo "macos"
      ;;
    *)
      die "unsupported OS for packaging: $(uname -s)"
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
      echo "armv7"
      ;;
    armv6l|armv6)
      echo "armv6"
      ;;
    *)
      die "unsupported architecture for packaging: ${machine}"
      ;;
  esac
}

default_release_target() {
  printf '%s-%s\n' "$(target_os)" "$(target_arch)"
}

default_cargo_target_for_release_target() {
  case "$1" in
    linux-x86_64)
      echo "x86_64-unknown-linux-musl"
      ;;
    linux-aarch64)
      echo "aarch64-unknown-linux-musl"
      ;;
    linux-armv7)
      echo "armv7-unknown-linux-musleabihf"
      ;;
    linux-armv6)
      echo "arm-unknown-linux-musleabihf"
      ;;
  esac
}

ensure_cargo_target_installed() {
  [[ -n "${CARGO_TARGET}" ]] || return 0
  command -v rustup >/dev/null 2>&1 || return 0

  if ! rustup target list --installed | grep -Fxq "${CARGO_TARGET}"; then
    log "installing Rust target ${CARGO_TARGET}"
    rustup target add "${CARGO_TARGET}"
  fi
}

build_binary() {
  if [[ "${SKIP_BUILD}" == "1" ]]; then
    return
  fi

  ensure_cargo_target_installed

  if [[ -n "${CARGO_TARGET}" ]]; then
    log "building release binary for Cargo target ${CARGO_TARGET}"
    cargo build --locked --release --target "${CARGO_TARGET}" --manifest-path "${ROOT}/Cargo.toml"
  else
    log "building release binary for host target"
    cargo build --locked --release --manifest-path "${ROOT}/Cargo.toml"
  fi
}

resolve_binary() {
  if [[ -n "${BIN_PATH}" ]]; then
    printf '%s\n' "${BIN_PATH}"
    return
  fi

  if [[ -n "${CARGO_TARGET}" ]]; then
    printf '%s\n' "${ROOT}/target/${CARGO_TARGET}/release/${APP_NAME}"
  else
    printf '%s\n' "${ROOT}/target/release/${APP_NAME}"
  fi
}

write_sha256() {
  local file="$1"
  local dir base
  dir="$(cd "$(dirname "${file}")" && pwd)"
  base="$(basename "${file}")"

  if command -v sha256sum >/dev/null 2>&1; then
    (cd "${dir}" && sha256sum "${base}" >"${base}.sha256")
  elif command -v shasum >/dev/null 2>&1; then
    (cd "${dir}" && shasum -a 256 "${base}" >"${base}.sha256")
  else
    die "sha256sum or shasum is required"
  fi
}

package_binary() {
  local binary="$1"
  local release_target="$2"
  local work_dir stage latest_name latest_path

  [[ -x "${binary}" ]] || die "binary is not executable: ${binary}"

  work_dir="$(mktemp -d)"
  stage="${work_dir}/${APP_NAME}-${release_target}"
  mkdir -p "${stage}" "${OUT_DIR}"
  cp "${binary}" "${stage}/${APP_NAME}"
  chmod 0755 "${stage}/${APP_NAME}"
  cp "${ROOT}/config.example.toml" "${stage}/config.example.toml"

  latest_name="${APP_NAME}-${release_target}.tar.gz"
  latest_path="${OUT_DIR}/${latest_name}"

  tar -czf "${latest_path}" -C "${work_dir}" "$(basename "${stage}")"
  write_sha256 "${latest_path}"

  rm -rf "${work_dir}"

  log "wrote ${latest_path}"
}

main() {
  require_cmd cargo
  require_cmd tar

  if [[ -z "${RELEASE_TARGET}" ]]; then
    RELEASE_TARGET="$(default_release_target)"
  fi

  if [[ -z "${CARGO_TARGET}" ]]; then
    CARGO_TARGET="$(default_cargo_target_for_release_target "${RELEASE_TARGET}")"
    if [[ -n "${CARGO_TARGET}" ]]; then
      log "using default Cargo target ${CARGO_TARGET} for ${RELEASE_TARGET}"
    fi
  fi

  build_binary
  package_binary "$(resolve_binary)" "${RELEASE_TARGET}"
}

main "$@"
