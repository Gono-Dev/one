#!/usr/bin/env bash
set -euo pipefail

APP_NAME="gono-cloud"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${GONO_CLOUD_RELEASE_DIR:-${ROOT}/dist}"
SKIP_BUILD="${GONO_CLOUD_SKIP_BUILD:-0}"
CARGO_TARGET="${GONO_CLOUD_CARGO_TARGET:-}"
PACKAGE_VERSION="${GONO_CLOUD_PACKAGE_VERSION:-}"
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

crate_version() {
  sed -n 's/^version = "\(.*\)"/\1/p' "${ROOT}/Cargo.toml" | head -n 1
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

build_binary() {
  if [[ "${SKIP_BUILD}" == "1" ]]; then
    return
  fi

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
  local version="$2"
  local release_target="$3"
  local work_dir stage latest_name versioned_name latest_path versioned_path

  [[ -x "${binary}" ]] || die "binary is not executable: ${binary}"

  work_dir="$(mktemp -d)"
  stage="${work_dir}/${APP_NAME}-${release_target}"
  mkdir -p "${stage}" "${OUT_DIR}"
  cp "${binary}" "${stage}/${APP_NAME}"
  chmod 0755 "${stage}/${APP_NAME}"
  cp "${ROOT}/config.example.toml" "${stage}/config.example.toml"

  latest_name="${APP_NAME}-${release_target}.tar.gz"
  versioned_name="${APP_NAME}-${version}-${release_target}.tar.gz"
  latest_path="${OUT_DIR}/${latest_name}"
  versioned_path="${OUT_DIR}/${versioned_name}"

  tar -czf "${latest_path}" -C "${work_dir}" "$(basename "${stage}")"
  cp "${latest_path}" "${versioned_path}"
  write_sha256 "${latest_path}"
  write_sha256 "${versioned_path}"

  rm -rf "${work_dir}"

  log "wrote ${latest_path}"
  log "wrote ${versioned_path}"
}

main() {
  require_cmd cargo
  require_cmd sed
  require_cmd tar

  if [[ -z "${PACKAGE_VERSION}" ]]; then
    PACKAGE_VERSION="$(crate_version)"
  fi
  [[ -n "${PACKAGE_VERSION}" ]] || die "could not determine package version"

  if [[ -z "${RELEASE_TARGET}" ]]; then
    RELEASE_TARGET="$(default_release_target)"
  fi

  build_binary
  package_binary "$(resolve_binary)" "${PACKAGE_VERSION}" "${RELEASE_TARGET}"
}

main "$@"
