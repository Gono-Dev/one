#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/gono-cloud-smoke.XXXXXX")"
SERVER_PID=""
RUN_LITMUS="${RUN_LITMUS:-0}"
KEEP_SMOKE_DIR="${KEEP_SMOKE_DIR:-0}"

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  if [[ "${KEEP_SMOKE_DIR}" != "1" ]]; then
    rm -rf "${WORK_DIR}"
  else
    echo "kept smoke work dir: ${WORK_DIR}"
  fi
}
trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

litmus_cmd() {
  if [[ -n "${LITMUS:-}" ]]; then
    if [[ ! -x "${LITMUS}" ]]; then
      echo "missing required command: ${LITMUS}" >&2
      exit 1
    fi
    echo "${LITMUS}"
    return
  fi

  require_cmd litmus
  command -v litmus
}

find_port() {
  if [[ -n "${GONE_CLOUD_SMOKE_PORT:-}" ]]; then
    echo "${GONE_CLOUD_SMOKE_PORT}"
    return
  fi

  python3 - <<'PY'
import socket
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

wait_for_server() {
  local url="$1"
  local log_file="$2"

  for _ in $(seq 1 100); do
    if curl -fsS "${url}/status.php" >/dev/null 2>&1; then
      return 0
    fi
    if [[ -n "${SERVER_PID}" ]] && ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      echo "server exited before becoming ready" >&2
      cat "${log_file}" >&2 || true
      exit 1
    fi
    sleep 0.1
  done

  echo "server did not become ready" >&2
  cat "${log_file}" >&2 || true
  exit 1
}

wait_for_password() {
  local log_file="$1"
  local password=""

  for _ in $(seq 1 100); do
    password="$(sed -n 's/.*Generated app password for gono: //p' "${log_file}" | tail -n 1)"
    if [[ -n "${password}" ]]; then
      echo "${password}"
      return 0
    fi
    sleep 0.1
  done

  echo "generated app password was not printed" >&2
  cat "${log_file}" >&2 || true
  exit 1
}

assert_status() {
  local expected="$1"
  local method="$2"
  local url="$3"
  shift 3

  local status
  status="$(curl -sS -o /dev/null -w '%{http_code}' -X "${method}" "$@" "${url}")"
  if [[ "${status}" != "${expected}" ]]; then
    echo "expected ${method} ${url} -> ${expected}, got ${status}" >&2
    exit 1
  fi
}

assert_status_any() {
  local expected_csv="$1"
  local method="$2"
  local url="$3"
  shift 3

  local status
  status="$(curl -sS -o /dev/null -w '%{http_code}' -X "${method}" "$@" "${url}")"
  IFS=',' read -r -a expected_codes <<<"${expected_csv}"
  for expected in "${expected_codes[@]}"; do
    if [[ "${status}" == "${expected}" ]]; then
      return 0
    fi
  done

  echo "expected ${method} ${url} -> one of ${expected_csv}, got ${status}" >&2
  exit 1
}

websocket_smoke() {
  BASE_URL="${BASE_URL}" PASSWORD="${PASSWORD}" python3 - <<'PY'
import base64
import os
import socket
import struct
from urllib.parse import urlparse

base_url = os.environ["BASE_URL"]
password = os.environ["PASSWORD"]
url = urlparse(base_url)
port = url.port or (443 if url.scheme == "https" else 80)
host = url.hostname
key = base64.b64encode(os.urandom(16)).decode("ascii")

def recv_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("websocket closed unexpectedly")
        data += chunk
    return data

def send_frame(sock, opcode, payload):
    payload = payload if isinstance(payload, bytes) else payload.encode("utf-8")
    header = bytearray([0x80 | opcode])
    length = len(payload)
    if length < 126:
        header.append(0x80 | length)
    elif length <= 0xFFFF:
        header.append(0x80 | 126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(0x80 | 127)
        header.extend(struct.pack("!Q", length))
    mask = os.urandom(4)
    masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(bytes(header) + mask + masked)

def read_frame(sock):
    first, second = recv_exact(sock, 2)
    opcode = first & 0x0F
    masked = second & 0x80
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(sock, 8))[0]
    mask = recv_exact(sock, 4) if masked else b""
    payload = recv_exact(sock, length)
    if masked:
        payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    return opcode, payload

with socket.create_connection((host, port), timeout=5) as sock:
    request = (
        "GET /push/ws HTTP/1.1\r\n"
        f"Host: {url.netloc}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    sock.sendall(request.encode("ascii"))
    response = b""
    while b"\r\n\r\n" not in response:
        response += sock.recv(4096)
    status_line = response.split(b"\r\n", 1)[0]
    if b" 101 " not in status_line:
        raise RuntimeError(f"websocket upgrade failed: {status_line!r}")

    send_frame(sock, 0x1, "gono")
    send_frame(sock, 0x1, password)
    while True:
        opcode, payload = read_frame(sock)
        if opcode == 0x1:
            text = payload.decode("utf-8")
            if text != "authenticated":
                raise RuntimeError(f"unexpected websocket text: {text!r}")
            send_frame(sock, 0x8, b"")
            break
        if opcode == 0x9:
            send_frame(sock, 0xA, payload)
        elif opcode == 0x8:
            raise RuntimeError("websocket closed before authentication")
PY
}

require_cmd cargo
require_cmd curl
require_cmd grep
require_cmd python3
require_cmd sed

PORT="$(find_port)"
BASE_URL="http://127.0.0.1:${PORT}"
CONFIG_FILE="${WORK_DIR}/config.toml"
LOG_FILE="${WORK_DIR}/server.log"

cat >"${CONFIG_FILE}" <<EOF
[server]
bind = "127.0.0.1:${PORT}"
cert_file = "${WORK_DIR}/cert.pem"
key_file = "${WORK_DIR}/key.pem"
base_url = "${BASE_URL}"

[storage]
data_dir = "${WORK_DIR}/data"
xattr_ns = "user.nc"
upload_min_free_bytes = 0
upload_min_free_percent = 0

[db]
path = "${WORK_DIR}/gono-cloud.db"
max_connections = 5

[auth]
realm = "Gono Cloud"

[logging]
loglevel = "warning"
logfile = ""
EOF

echo "building gono-cloud"
cargo build --quiet --manifest-path "${ROOT}/Cargo.toml"

echo "starting temporary gono-cloud on ${BASE_URL}"
GONE_CLOUD_CONFIG="${CONFIG_FILE}" \
GONE_CLOUD_INSECURE_HTTP=1 \
"${ROOT}/target/debug/gono-cloud" >"${LOG_FILE}" 2>&1 &
SERVER_PID="$!"

wait_for_server "${BASE_URL}" "${LOG_FILE}"
PASSWORD="$(wait_for_password "${LOG_FILE}")"
AUTH="gono:${PASSWORD}"
DAV_URL="${BASE_URL}/remote.php/dav"

echo "checking public endpoints"
curl -fsS "${BASE_URL}/status.php" | grep -q '"installed":true'
curl -fsS "${BASE_URL}/ocs/v2.php/cloud/capabilities" | grep -q '"chunking":"1.0"'
curl -fsS "${BASE_URL}/ocs/v2.php/cloud/capabilities" | grep -q '"notify_push"'
curl -fsS "${BASE_URL}/index.php/ocs/v2.php/cloud/capabilities" | grep -q '"chunking":"1.0"'

echo "checking OCS user endpoints"
assert_status "401" "GET" "${BASE_URL}/ocs/v2.php/cloud/user"
curl -fsS -u "${AUTH}" "${BASE_URL}/ocs/v2.php/cloud/user" | grep -q '"displayname":"gono"'
curl -fsS -u "${AUTH}" "${BASE_URL}/index.php/ocs/v2.php/cloud/user" | grep -q '"displayname":"gono"'
curl -fsS -u "${AUTH}" "${BASE_URL}/ocs/v2.php/cloud/users" | grep -q '"gono"'
assert_status "401" "GET" "${BASE_URL}/ocs/v1.php/cloud/user"
curl -fsS -u "${AUTH}" "${BASE_URL}/ocs/v1.php/cloud/user" | grep -q '"displayname":"gono"'
curl -fsS "${BASE_URL}/index.php/ocs/v1.php/cloud/capabilities" | grep -q '"chunking":"1.0"'

echo "checking OCS v2 compatibility placeholders"
assert_status "401" "GET" "${BASE_URL}/ocs/v2.php/apps/files_sharing/api/v1/shares"
curl -fsS -u "${AUTH}" "${BASE_URL}/ocs/v2.php/apps/files_sharing/api/v1/shares" | grep -q '"data":\[\]'
curl -fsS -u "${AUTH}" "${BASE_URL}/ocs/v2.php/apps/notifications/api/v2/notifications" | grep -q '"data":\[\]'
curl -fsS -u "${AUTH}" "${BASE_URL}/ocs/v2.php/apps/user_status/api/v1/user_status" | grep -q '"status":"online"'
assert_status "501" "POST" "${BASE_URL}/ocs/v2.php/apps/dav/api/v1/direct" -u "${AUTH}" -H "OCS-APIRequest: true"
assert_status "412" "POST" "${BASE_URL}/ocs/v2.php/translation/translate" -u "${AUTH}" -H "OCS-APIRequest: true"

echo "checking notify_push websocket"
websocket_smoke

echo "checking Basic Auth"
assert_status "401" "PROPFIND" "${DAV_URL}/" -H "Depth: 0"
assert_status "401" "PROPFIND" "${BASE_URL}/" -H "Depth: 0"

echo "checking PROPFIND"
curl -fsS -u "${AUTH}" \
  -X PROPFIND \
  -H "Depth: 0" \
  -H "Content-Type: application/xml" \
  --data '<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns"><d:prop><d:getetag/><oc:fileid/><oc:permissions/></d:prop></d:propfind>' \
  "${DAV_URL}/" | grep -q 'oc:fileid'

echo "checking root WebDAV compatibility"
curl -fsS -u "${AUTH}" \
  -X PROPFIND \
  -H "Depth: 0" \
  -H "Content-Type: application/xml" \
  --data '<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns"><d:prop><d:getetag/><oc:fileid/><oc:permissions/></d:prop></d:propfind>' \
  "${BASE_URL}/" | grep -q 'oc:fileid'
curl -fsS -u "${AUTH}" -X PUT --data-binary 'root smoke' "${BASE_URL}/root-smoke.txt" >/dev/null
curl -fsS -u "${AUTH}" "${DAV_URL}/root-smoke.txt" | grep -q 'root smoke'

echo "checking standard Gono Cloud files WebDAV compatibility"
curl -fsS -u "${AUTH}" \
  -X PROPFIND \
  -H "Depth: 0" \
  -H "Content-Type: application/xml" \
  --data '<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns"><d:prop><d:getetag/><oc:fileid/><oc:permissions/></d:prop></d:propfind>' \
  "${DAV_URL}/files/gono/" | grep -q 'oc:fileid'
curl -fsS -u "${AUTH}" -X PUT --data-binary 'standard gono cloud smoke' "${DAV_URL}/files/gono/standard-smoke.txt" >/dev/null
curl -fsS -u "${AUTH}" "${DAV_URL}/standard-smoke.txt" | grep -q 'standard gono cloud smoke'
assert_status_any "201,204" "COPY" "${DAV_URL}/files/gono/standard-smoke.txt" \
  -u "${AUTH}" \
  -H "Destination: ${DAV_URL}/files/gono/standard-smoke-copy.txt"
assert_status_any "201,204" "MOVE" "${DAV_URL}/files/gono/standard-smoke-copy.txt" \
  -u "${AUTH}" \
  -H "Destination: ${DAV_URL}/files/gono/standard-smoke-moved.txt"
assert_status "204" "DELETE" "${DAV_URL}/files/gono/standard-smoke-moved.txt" -u "${AUTH}"

echo "checking basic WebDAV writes"
curl -fsS -u "${AUTH}" -X PUT --data-binary 'hello smoke' "${DAV_URL}/smoke.txt" >/dev/null
curl -fsS -u "${AUTH}" "${DAV_URL}/smoke.txt" | grep -q 'hello smoke'
assert_status_any "201,204" "COPY" "${DAV_URL}/smoke.txt" \
  -u "${AUTH}" \
  -H "Destination: ${DAV_URL}/smoke-copy.txt"
assert_status_any "201,204" "MOVE" "${DAV_URL}/smoke-copy.txt" \
  -u "${AUTH}" \
  -H "Destination: ${DAV_URL}/smoke-moved.txt"
curl -fsS -u "${AUTH}" "${DAV_URL}/smoke-moved.txt" | grep -q 'hello smoke'
assert_status "204" "DELETE" "${DAV_URL}/smoke-moved.txt" -u "${AUTH}"

echo "checking chunking v2"
UPLOAD_URL="${DAV_URL}/uploads/gono/smoke-upload"
DESTINATION="${DAV_URL}/chunked-smoke.txt"
assert_status "201" "MKCOL" "${UPLOAD_URL}" \
  -u "${AUTH}" \
  -H "Destination: ${DESTINATION}" \
  -H "OC-Total-Length: 11"
assert_status "201" "PUT" "${UPLOAD_URL}/1" \
  -u "${AUTH}" \
  -H "Destination: ${DESTINATION}" \
  -H "OC-Total-Length: 11" \
  --data-binary 'hello '
assert_status "201" "PUT" "${UPLOAD_URL}/2" \
  -u "${AUTH}" \
  -H "Destination: ${DESTINATION}" \
  -H "OC-Total-Length: 11" \
  --data-binary 'world'
assert_status_any "201,204" "MOVE" "${UPLOAD_URL}/.file" \
  -u "${AUTH}" \
  -H "Destination: ${DESTINATION}"
curl -fsS -u "${AUTH}" "${DESTINATION}" | grep -q 'hello world'

echo "checking metrics"
curl -fsS -u "${AUTH}" "${BASE_URL}/metrics" | grep -q 'gono_cloud_sync_token'

if [[ "${RUN_LITMUS}" == "1" ]]; then
  LITMUS_BIN="$(litmus_cmd)"
  echo "running litmus against ${DAV_URL}/"
  (cd "${WORK_DIR}" && "${LITMUS_BIN}" "${DAV_URL}/" gono "${PASSWORD}")
fi

echo "compat smoke passed"
