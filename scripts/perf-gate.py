#!/usr/bin/env python3
import argparse
import base64
import csv
import hashlib
import http.client
import json
import os
import random
import re
import socket
import ssl
import statistics
import struct
import subprocess
import sys
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None


PROPFIND_BODY = b"""<?xml version="1.0"?>
<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:prop>
    <d:getetag/>
    <d:getcontentlength/>
    <oc:fileid/>
    <oc:permissions/>
  </d:prop>
</d:propfind>"""


def sync_collection_body(sync_token: str = "0") -> bytes:
    return f"""<?xml version="1.0"?>
<d:sync-collection xmlns:d="DAV:">
  <d:sync-token>{sync_token}</d:sync-token>
  <d:sync-level>1</d:sync-level>
  <d:prop>
    <d:getetag />
    <oc:fileid xmlns:oc="http://owncloud.org/ns" />
    <oc:favorite xmlns:oc="http://owncloud.org/ns" />
  </d:prop>
</d:sync-collection>""".encode("utf-8")


@dataclass(frozen=True)
class Profile:
    baseline_duration: int
    mixed_duration: int
    mixed_concurrency: int
    seed_files: int
    seed_concurrency: int
    file_size_kb: int
    chunk_duration: int
    chunk_concurrency: int
    chunk_file_mb: int
    chunk_piece_mb: int
    notify_duration: int
    ws_connections: int
    event_rate: int
    admin_duration: int
    admin_concurrency: int
    spike_duration: int
    spike_http: int
    spike_ws: int
    soak_duration: int
    soak_http: int
    soak_ws: int


PROFILES = {
    "quick": Profile(
        baseline_duration=30,
        mixed_duration=60,
        mixed_concurrency=8,
        seed_files=40,
        seed_concurrency=4,
        file_size_kb=16,
        chunk_duration=60,
        chunk_concurrency=2,
        chunk_file_mb=4,
        chunk_piece_mb=1,
        notify_duration=60,
        ws_connections=20,
        event_rate=5,
        admin_duration=60,
        admin_concurrency=2,
        spike_duration=60,
        spike_http=24,
        spike_ws=40,
        soak_duration=300,
        soak_http=8,
        soak_ws=20,
    ),
    "medium": Profile(
        baseline_duration=600,
        mixed_duration=2700,
        mixed_concurrency=150,
        seed_files=20_000,
        seed_concurrency=16,
        file_size_kb=64,
        chunk_duration=1800,
        chunk_concurrency=20,
        chunk_file_mb=256,
        chunk_piece_mb=8,
        notify_duration=2700,
        ws_connections=1000,
        event_rate=100,
        admin_duration=1200,
        admin_concurrency=8,
        spike_duration=900,
        spike_http=300,
        spike_ws=2000,
        soak_duration=28_800,
        soak_http=100,
        soak_ws=1000,
    ),
}


THRESHOLDS_MS = {
    "status": 100,
    "capabilities": 100,
    "propfind0": 150,
    "propfind1": 700,
    "put_small": 800,
    "sync_report": 1500,
    "ws_auth": 800,
    "ws_message": 400,
}


class StopFlag:
    def __init__(self) -> None:
        self._event = threading.Event()

    def stop(self) -> None:
        self._event.set()

    def stopped(self) -> bool:
        return self._event.is_set()

    def wait(self, seconds: float) -> bool:
        return self._event.wait(seconds)


class Recorder:
    def __init__(self, report_dir: Path) -> None:
        self.report_dir = report_dir
        self.raw_dir = report_dir / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.latencies: Dict[str, List[float]] = defaultdict(list)
        self.counts: Counter[str] = Counter()
        self.statuses: Counter[str] = Counter()
        self.failures: List[dict] = []
        self.bytes_read = 0
        self.bytes_written = 0
        self.events_file = (self.raw_dir / "events.jsonl").open("a", encoding="utf-8")

    def close(self) -> None:
        with self.lock:
            self.events_file.close()

    def record(
        self,
        scenario: str,
        op: str,
        status: Optional[int],
        latency_ms: float,
        ok: bool,
        bytes_read: int = 0,
        bytes_written: int = 0,
        error: Optional[str] = None,
    ) -> None:
        event = {
            "ts": time.time(),
            "scenario": scenario,
            "op": op,
            "status": status,
            "latency_ms": round(latency_ms, 3),
            "ok": ok,
            "bytes_read": bytes_read,
            "bytes_written": bytes_written,
            "error": error,
        }
        with self.lock:
            self.counts[op] += 1
            self.latencies[op].append(latency_ms)
            if status is not None:
                self.statuses[f"{op}:{status}"] += 1
            self.bytes_read += bytes_read
            self.bytes_written += bytes_written
            if not ok:
                self.failures.append(event)
            self.events_file.write(json.dumps(event, sort_keys=True) + "\n")
            if len(self.failures) < 2000 or ok:
                self.events_file.flush()

    def summary(self) -> Dict[str, dict]:
        with self.lock:
            result = {}
            for op, values in self.latencies.items():
                sorted_values = sorted(values)
                result[op] = {
                    "count": len(sorted_values),
                    "p50_ms": percentile(sorted_values, 50),
                    "p95_ms": percentile(sorted_values, 95),
                    "p99_ms": percentile(sorted_values, 99),
                    "max_ms": max(sorted_values) if sorted_values else 0,
                }
            return result


class HttpClient:
    def __init__(
        self,
        base_url: str,
        user: str,
        password: str,
        insecure_tls: bool = True,
        timeout: float = 60.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.parsed = urlparse(self.base_url)
        self.user = user
        self.password = password
        self.timeout = timeout
        self.insecure_tls = insecure_tls
        self.local = threading.local()
        token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        self.auth_header = f"Basic {token}"

    def absolute_url(self, path: str) -> str:
        return urljoin(self.base_url + "/", path.lstrip("/"))

    def _connection(self) -> http.client.HTTPConnection:
        conn = getattr(self.local, "conn", None)
        if conn is not None:
            return conn
        host = self.parsed.hostname or "127.0.0.1"
        port = self.parsed.port
        if self.parsed.scheme == "https":
            context = ssl._create_unverified_context() if self.insecure_tls else None
            conn = http.client.HTTPSConnection(host, port, timeout=self.timeout, context=context)
        else:
            conn = http.client.HTTPConnection(host, port, timeout=self.timeout)
        self.local.conn = conn
        return conn

    def close_thread_connection(self) -> None:
        conn = getattr(self.local, "conn", None)
        if conn is not None:
            conn.close()
            self.local.conn = None

    def request(
        self,
        method: str,
        path: str,
        body: Optional[bytes] = None,
        headers: Optional[dict] = None,
        expected: Optional[Iterable[int]] = None,
    ) -> Tuple[Optional[int], bytes, float, bool, Optional[str]]:
        expected_set = set(expected or range(200, 300))
        request_headers = {
            "Authorization": self.auth_header,
            "User-Agent": "gono-cloud-perf/1",
        }
        if headers:
            request_headers.update(headers)
        if body is not None and "Content-Length" not in request_headers:
            request_headers["Content-Length"] = str(len(body))
        target = path if path.startswith("/") else "/" + path
        start = time.perf_counter()
        try:
            conn = self._connection()
            conn.request(method, target, body=body, headers=request_headers)
            response = conn.getresponse()
            data = response.read()
            latency_ms = (time.perf_counter() - start) * 1000.0
            ok = response.status in expected_set
            return response.status, data, latency_ms, ok, None
        except Exception as err:
            self.close_thread_connection()
            latency_ms = (time.perf_counter() - start) * 1000.0
            return None, b"", latency_ms, False, repr(err)


class PathPool:
    def __init__(self, base_path: str) -> None:
        self.base_path = base_path.rstrip("/")
        self.lock = threading.Lock()
        self.files: List[str] = []
        self.counter = 0

    def next_path(self, suffix: str = ".bin") -> str:
        with self.lock:
            self.counter += 1
            value = self.counter
        bucket = value % 100
        return f"{self.base_path}/d{bucket:03d}/f{value:08d}{suffix}"

    def add(self, path: str) -> None:
        with self.lock:
            self.files.append(path)

    def sample(self) -> Optional[str]:
        with self.lock:
            if not self.files:
                return None
            return random.choice(self.files)

    def replace(self, old: str, new: str) -> None:
        with self.lock:
            try:
                index = self.files.index(old)
                self.files[index] = new
            except ValueError:
                self.files.append(new)

    def remove(self, path: str) -> None:
        with self.lock:
            try:
                self.files.remove(path)
            except ValueError:
                pass

    def snapshot(self) -> List[str]:
        with self.lock:
            return list(self.files)


class Sampler:
    def __init__(
        self,
        client: HttpClient,
        recorder: Recorder,
        stop: StopFlag,
        interval: int,
        metrics_interval: int,
        pid: Optional[int],
        db_path: Optional[str],
    ) -> None:
        self.client = client
        self.recorder = recorder
        self.stop = stop
        self.interval = interval
        self.metrics_interval = metrics_interval
        self.pid = pid
        self.db_path = db_path
        self.thread = threading.Thread(target=self._run, name="perf-sampler", daemon=True)
        self.system_file = recorder.report_dir / "system.csv"
        self.metrics_file = recorder.report_dir / "metrics.prom"

    def start(self) -> None:
        self.thread.start()

    def join(self) -> None:
        self.thread.join(timeout=2)

    def _run(self) -> None:
        next_metrics = 0.0
        with self.system_file.open("a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "ts",
                    "pid",
                    "rss_kb",
                    "cpu_percent",
                    "fd_count",
                    "db_bytes",
                    "wal_bytes",
                    "shm_bytes",
                ],
            )
            writer.writeheader()
            while not self.stop.stopped():
                writer.writerow(self._sample_system())
                file.flush()
                now = time.time()
                if now >= next_metrics:
                    self._sample_metrics()
                    next_metrics = now + self.metrics_interval
                self.stop.wait(self.interval)

    def _sample_system(self) -> dict:
        pid = self.pid or find_gono_cloud_pid()
        row = {
            "ts": time.time(),
            "pid": pid or "",
            "rss_kb": "",
            "cpu_percent": "",
            "fd_count": "",
            "db_bytes": "",
            "wal_bytes": "",
            "shm_bytes": "",
        }
        if pid:
            try:
                output = subprocess.check_output(
                    ["ps", "-o", "rss=,%cpu=", "-p", str(pid)],
                    text=True,
                    stderr=subprocess.DEVNULL,
                ).strip()
                if output:
                    parts = output.split()
                    if len(parts) >= 2:
                        row["rss_kb"] = parts[0]
                        row["cpu_percent"] = parts[1]
            except Exception:
                pass
            try:
                output = subprocess.check_output(
                    ["lsof", "-p", str(pid)],
                    text=True,
                    stderr=subprocess.DEVNULL,
                )
                row["fd_count"] = max(0, len(output.splitlines()) - 1)
            except Exception:
                pass
        if self.db_path:
            db = Path(self.db_path)
            row["db_bytes"] = file_size(db)
            row["wal_bytes"] = file_size(Path(str(db) + "-wal"))
            row["shm_bytes"] = file_size(Path(str(db) + "-shm"))
        return row

    def _sample_metrics(self) -> None:
        status, data, latency, ok, error = self.client.request("GET", "/metrics")
        self.recorder.record(
            "sampler",
            "metrics",
            status,
            latency,
            ok,
            bytes_read=len(data),
            error=error,
        )
        if ok:
            with self.metrics_file.open("ab") as file:
                file.write(f"# scrape_ts {time.time()}\n".encode("ascii"))
                file.write(data)
                if not data.endswith(b"\n"):
                    file.write(b"\n")


def percentile(sorted_values: List[float], pct: int) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return round(sorted_values[0], 3)
    index = (len(sorted_values) - 1) * (pct / 100.0)
    lower = int(index)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = index - lower
    value = sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight
    return round(value, 3)


def file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def find_gono_cloud_pid() -> Optional[int]:
    for cmd in (["pgrep", "-x", "gono-cloud"], ["pgrep", "-f", "gono-cloud"]):
        try:
            output = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
            for line in output.splitlines():
                value = line.strip()
                if value and value.isdigit() and int(value) != os.getpid():
                    return int(value)
        except Exception:
            pass
    return None


def load_db_path(config_path: Optional[str]) -> Optional[str]:
    if not config_path or tomllib is None:
        return None
    try:
        with open(config_path, "rb") as file:
            data = tomllib.load(file)
        db_path = data.get("db", {}).get("path")
        if not db_path:
            return None
        if os.path.isabs(db_path):
            return db_path
        return str(Path(config_path).parent / db_path)
    except Exception:
        return None


def make_payload(size_kb: int, seed: int = 1) -> bytes:
    unit = hashlib.sha256(f"gono-cloud-perf-{seed}".encode("ascii")).digest()
    size = max(1, size_kb) * 1024
    return (unit * ((size // len(unit)) + 1))[:size]


def ensure_perf_dirs(client: HttpClient, recorder: Recorder, pool: PathPool) -> None:
    status, data, latency, ok, error = client.request(
        "MKCOL",
        pool.base_path,
        expected={201, 204, 405},
    )
    recorder.record("setup", "mkcol_root", status, latency, ok, len(data), error=error)
    for index in range(100):
        path = f"{pool.base_path}/d{index:03d}"
        status, data, latency, ok, error = client.request(
            "MKCOL",
            path,
            expected={201, 204, 405},
        )
        recorder.record("setup", "mkcol_bucket", status, latency, ok, len(data), error=error)


def preseed_files(
    client: HttpClient,
    recorder: Recorder,
    pool: PathPool,
    count: int,
    concurrency: int,
    file_size_kb: int,
) -> None:
    if count <= 0:
        return
    payload = make_payload(file_size_kb, 11)
    existing = len(pool.snapshot())
    if existing >= count:
        return
    needed = count - existing

    def put_one(_: int) -> None:
        path = pool.next_path()
        status, data, latency, ok, error = client.request(
            "PUT",
            path,
            body=payload,
            headers={"Content-Type": "application/octet-stream"},
            expected={201, 204},
        )
        recorder.record(
            "setup",
            "seed_put",
            status,
            latency,
            ok,
            bytes_read=len(data),
            bytes_written=len(payload),
            error=error,
        )
        if ok:
            pool.add(path)

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        futures = [executor.submit(put_one, i) for i in range(needed)]
        for future in as_completed(futures):
            future.result()


def record_request(
    recorder: Recorder,
    scenario: str,
    op: str,
    client: HttpClient,
    method: str,
    path: str,
    body: Optional[bytes] = None,
    headers: Optional[dict] = None,
    expected: Optional[Iterable[int]] = None,
) -> Tuple[bool, Optional[int], bytes]:
    status, data, latency, ok, error = client.request(method, path, body, headers, expected)
    recorder.record(
        scenario,
        op,
        status,
        latency,
        ok,
        bytes_read=len(data),
        bytes_written=len(body or b""),
        error=error,
    )
    return ok, status, data


def run_baseline(client: HttpClient, recorder: Recorder, pool: PathPool, profile: Profile) -> None:
    ensure_perf_dirs(client, recorder, pool)
    payload = make_payload(profile.file_size_kb, 21)
    path = f"{pool.base_path}/d000/baseline.txt"
    record_request(recorder, "baseline", "status", client, "GET", "/status.php")
    record_request(recorder, "baseline", "capabilities", client, "GET", "/ocs/v2.php/cloud/capabilities")
    record_request(recorder, "baseline", "put_small", client, "PUT", path, payload, expected={201, 204})
    pool.add(path)
    record_request(
        recorder,
        "baseline",
        "propfind0",
        client,
        "PROPFIND",
        path,
        PROPFIND_BODY,
        headers={"Depth": "0", "Content-Type": "application/xml"},
        expected={207},
    )
    record_request(
        recorder,
        "baseline",
        "propfind1",
        client,
        "PROPFIND",
        pool.base_path + "/d000/",
        PROPFIND_BODY,
        headers={"Depth": "1", "Content-Type": "application/xml"},
        expected={207},
    )
    record_request(recorder, "baseline", "head", client, "HEAD", path)
    record_request(recorder, "baseline", "get", client, "GET", path)
    copy_path = f"{pool.base_path}/d000/baseline-copy.txt"
    record_request(
        recorder,
        "baseline",
        "copy",
        client,
        "COPY",
        path,
        headers={"Destination": client.absolute_url(copy_path)},
        expected={201, 204},
    )
    move_path = f"{pool.base_path}/d000/baseline-moved.txt"
    record_request(
        recorder,
        "baseline",
        "move",
        client,
        "MOVE",
        copy_path,
        headers={"Destination": client.absolute_url(move_path)},
        expected={201, 204},
    )
    record_request(
        recorder,
        "baseline",
        "sync_report",
        client,
        "REPORT",
        "/remote.php/dav/",
        sync_collection_body("0"),
        headers={"Depth": "0", "Content-Type": "application/xml"},
        expected={207},
    )
    run_one_chunk_upload(client, recorder, pool, "baseline", 2, 1)
    record_request(recorder, "baseline", "delete", client, "DELETE", move_path, expected={204, 404})


def run_mixed_load(
    client: HttpClient,
    recorder: Recorder,
    pool: PathPool,
    scenario: str,
    duration: int,
    concurrency: int,
    file_size_kb: int,
) -> None:
    payload = make_payload(file_size_kb, 31)
    stop_at = time.time() + duration
    weights = [
        ("propfind0", 18),
        ("propfind1", 12),
        ("head", 15),
        ("get", 20),
        ("put_small", 15),
        ("move", 5),
        ("copy", 5),
        ("delete", 5),
        ("sync_report", 5),
    ]
    choices = [name for name, weight in weights for _ in range(weight)]

    def worker(_: int) -> None:
        while time.time() < stop_at:
            op = random.choice(choices)
            path = pool.sample()
            if op in {"propfind0", "head", "get", "copy", "move", "delete"} and path is None:
                time.sleep(0.01)
                continue
            if op == "propfind0":
                record_request(
                    recorder,
                    scenario,
                    op,
                    client,
                    "PROPFIND",
                    path or pool.base_path,
                    PROPFIND_BODY,
                    {"Depth": "0", "Content-Type": "application/xml"},
                    {207, 404},
                )
            elif op == "propfind1":
                record_request(
                    recorder,
                    scenario,
                    op,
                    client,
                    "PROPFIND",
                    f"{pool.base_path}/d{random.randrange(100):03d}/",
                    PROPFIND_BODY,
                    {"Depth": "1", "Content-Type": "application/xml"},
                    {207},
                )
            elif op == "head":
                record_request(
                    recorder,
                    scenario,
                    op,
                    client,
                    "HEAD",
                    path or pool.base_path,
                    expected={200, 404},
                )
            elif op == "get":
                record_request(
                    recorder,
                    scenario,
                    op,
                    client,
                    "GET",
                    path or pool.base_path,
                    expected={200, 404},
                )
            elif op == "put_small":
                new_path = pool.next_path()
                ok, status, _ = record_request(
                    recorder,
                    scenario,
                    op,
                    client,
                    "PUT",
                    new_path,
                    payload,
                    {"Content-Type": "application/octet-stream"},
                    {201, 204},
                )
                if ok and status in {201, 204}:
                    pool.add(new_path)
            elif op == "copy" and path:
                new_path = pool.next_path()
                ok, status, _ = record_request(
                    recorder,
                    scenario,
                    op,
                    client,
                    "COPY",
                    path,
                    headers={"Destination": client.absolute_url(new_path)},
                    expected={201, 204, 404},
                )
                if ok and status in {201, 204}:
                    pool.add(new_path)
            elif op == "move" and path:
                new_path = pool.next_path()
                ok, _, _ = record_request(
                    recorder,
                    scenario,
                    op,
                    client,
                    "MOVE",
                    path,
                    headers={"Destination": client.absolute_url(new_path)},
                    expected={201, 204, 404},
                )
                if ok and status in {201, 204}:
                    pool.replace(path, new_path)
            elif op == "delete" and path:
                ok, _, _ = record_request(
                    recorder,
                    scenario,
                    op,
                    client,
                    "DELETE",
                    path,
                    expected={204, 404},
                )
                if ok:
                    pool.remove(path)
            elif op == "sync_report":
                record_request(
                    recorder,
                    scenario,
                    op,
                    client,
                    "REPORT",
                    "/remote.php/dav/",
                    sync_collection_body("0"),
                    {"Depth": "0", "Content-Type": "application/xml"},
                    {207, 403},
                )

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        futures = [executor.submit(worker, i) for i in range(concurrency)]
        for future in as_completed(futures):
            future.result()


def run_one_chunk_upload(
    client: HttpClient,
    recorder: Recorder,
    pool: PathPool,
    scenario: str,
    file_mb: int,
    piece_mb: int,
) -> None:
    upload_id = f"upload-{int(time.time() * 1000)}-{random.randrange(1_000_000)}"
    upload_path = f"/remote.php/dav/uploads/{quote(client.user)}/{upload_id}"
    destination = pool.next_path(".chunked")
    total_bytes = max(1, file_mb) * 1024 * 1024
    piece_bytes = max(1, piece_mb) * 1024 * 1024
    piece = make_payload(piece_bytes // 1024, random.randrange(10_000))
    headers = {
        "Destination": client.absolute_url(destination),
        "OC-Total-Length": str(total_bytes),
    }
    ok, _, _ = record_request(
        recorder,
        scenario,
        "chunk_mkcol",
        client,
        "MKCOL",
        upload_path,
        headers=headers,
        expected={201, 204, 405},
    )
    if not ok:
        return
    chunk_index = 1
    remaining = total_bytes
    while remaining > 0:
        body = piece if remaining >= len(piece) else piece[:remaining]
        ok, _, _ = record_request(
            recorder,
            scenario,
            "chunk_put",
            client,
            "PUT",
            f"{upload_path}/{chunk_index}",
            body=body,
            headers=headers,
            expected={201, 204},
        )
        if not ok:
            return
        remaining -= len(body)
        chunk_index += 1
    ok, _, _ = record_request(
        recorder,
        scenario,
        "chunk_move",
        client,
        "MOVE",
        f"{upload_path}/.file",
        headers={"Destination": client.absolute_url(destination)},
        expected={201, 204},
    )
    if ok:
        pool.add(destination)
        record_request(recorder, scenario, "chunk_head", client, "HEAD", destination)


def run_chunking(
    client: HttpClient,
    recorder: Recorder,
    pool: PathPool,
    duration: int,
    concurrency: int,
    file_mb: int,
    piece_mb: int,
) -> None:
    stop_at = time.time() + duration

    def worker(_: int) -> None:
        while time.time() < stop_at:
            run_one_chunk_upload(client, recorder, pool, "chunking", file_mb, piece_mb)

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        futures = [executor.submit(worker, i) for i in range(concurrency)]
        for future in as_completed(futures):
            future.result()


def websocket_frame(opcode: int, payload: bytes) -> bytes:
    first = bytes([0x80 | opcode])
    length = len(payload)
    if length < 126:
        header = bytearray([0x80 | length])
    elif length <= 0xFFFF:
        header = bytearray([0x80 | 126])
        header.extend(struct.pack("!H", length))
    else:
        header = bytearray([0x80 | 127])
        header.extend(struct.pack("!Q", length))
    mask = os.urandom(4)
    masked = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
    return first + bytes(header) + mask + masked


def websocket_read(sock: socket.socket) -> Tuple[int, bytes]:
    header = recv_exact(sock, 2)
    first, second = header[0], header[1]
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
        payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
    return opcode, payload


def recv_exact(sock: socket.socket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("socket closed")
        data.extend(chunk)
    return bytes(data)


def websocket_worker(
    client: HttpClient,
    recorder: Recorder,
    stop: StopFlag,
    scenario: str,
    index: int,
    ws_path: str,
) -> None:
    parsed = client.parsed
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    start = time.perf_counter()
    sock: Optional[socket.socket] = None
    try:
        raw = socket.create_connection((host, port), timeout=20)
        if parsed.scheme == "https":
            raw = ssl._create_unverified_context().wrap_socket(raw, server_hostname=host)
        sock = raw
        request = (
            f"GET {ws_path} HTTP/1.1\r\n"
            f"Host: {parsed.netloc}\r\n"
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
        if b" 101 " not in response.split(b"\r\n", 1)[0]:
            raise RuntimeError(response.split(b"\r\n", 1)[0].decode("latin1"))
        sock.sendall(websocket_frame(0x1, client.user.encode("utf-8")))
        sock.sendall(websocket_frame(0x1, client.password.encode("utf-8")))
        while True:
            opcode, payload = websocket_read(sock)
            if opcode == 0x1 and payload.decode("utf-8", "replace") == "authenticated":
                latency = (time.perf_counter() - start) * 1000.0
                recorder.record(scenario, "ws_auth", 101, latency, True)
                break
            if opcode == 0x9:
                sock.sendall(websocket_frame(0xA, payload))
            if opcode == 0x8:
                raise RuntimeError("closed before auth")
        sock.settimeout(1.0)
        while not stop.stopped():
            try:
                msg_start = time.perf_counter()
                opcode, payload = websocket_read(sock)
                latency = (time.perf_counter() - msg_start) * 1000.0
                if opcode == 0x1:
                    recorder.record(scenario, "ws_message", 200, latency, True, bytes_read=len(payload))
                elif opcode == 0x9:
                    sock.sendall(websocket_frame(0xA, payload))
                elif opcode == 0x8:
                    break
            except socket.timeout:
                continue
    except Exception as err:
        latency = (time.perf_counter() - start) * 1000.0
        recorder.record(scenario, "ws_auth", None, latency, False, error=f"ws{index}: {err!r}")
    finally:
        if sock is not None:
            try:
                sock.sendall(websocket_frame(0x8, b""))
            except Exception:
                pass
            try:
                sock.close()
            except Exception:
                pass


def run_notify(
    client: HttpClient,
    recorder: Recorder,
    pool: PathPool,
    duration: int,
    ws_connections: int,
    event_rate: int,
    file_size_kb: int,
    ws_path: str,
    scenario: str = "notify",
) -> None:
    stop = StopFlag()
    try:
        threading.stack_size(256 * 1024)
    except Exception:
        pass
    ws_executor = ThreadPoolExecutor(max_workers=max(1, ws_connections))
    ws_futures = [
        ws_executor.submit(websocket_worker, client, recorder, stop, scenario, index, ws_path)
        for index in range(ws_connections)
    ]
    payload = make_payload(file_size_kb, 41)
    end_at = time.time() + duration
    interval = 1.0 / max(1, event_rate)
    try:
        while time.time() < end_at:
            path = pool.next_path(".notify")
            ok, _, _ = record_request(
                recorder,
                scenario,
                "notify_put",
                client,
                "PUT",
                path,
                payload,
                {"Content-Type": "application/octet-stream"},
                {201, 204},
            )
            if ok:
                pool.add(path)
            time.sleep(interval)
    finally:
        stop.stop()
        for future in as_completed(ws_futures):
            future.result()
        ws_executor.shutdown(wait=True)


def run_admin_read(
    client: HttpClient,
    recorder: Recorder,
    duration: int,
    concurrency: int,
) -> None:
    stop_at = time.time() + duration

    def worker(_: int) -> None:
        while time.time() < stop_at:
            record_request(recorder, "admin", "admin_users", client, "GET", "/admin/users", expected={200, 403, 404})
            record_request(recorder, "admin", "admin_settings", client, "GET", "/admin/settings", expected={200, 403, 404})

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        futures = [executor.submit(worker, i) for i in range(concurrency)]
        for future in as_completed(futures):
            future.result()


def run_spike(
    client: HttpClient,
    recorder: Recorder,
    pool: PathPool,
    profile: Profile,
    ws_path: str,
) -> None:
    third = max(20, profile.spike_duration // 3)
    run_mixed_load(client, recorder, pool, "spike-ramp", third, max(1, profile.spike_http // 3), profile.file_size_kb)
    notify_thread = threading.Thread(
        target=run_notify,
        args=(
            client,
            recorder,
            pool,
            third,
            profile.spike_ws,
            max(1, profile.event_rate),
            profile.file_size_kb,
            ws_path,
            "spike-notify",
        ),
        daemon=True,
    )
    notify_thread.start()
    run_mixed_load(client, recorder, pool, "spike-peak", third, profile.spike_http, profile.file_size_kb)
    notify_thread.join()
    time.sleep(min(300, third))
    record_request(recorder, "spike", "metrics", client, "GET", "/metrics")


def run_soak(
    client: HttpClient,
    recorder: Recorder,
    pool: PathPool,
    profile: Profile,
    restart_command: Optional[str],
    ws_path: str,
) -> None:
    half = max(1, profile.soak_duration // 2)
    notify_thread = threading.Thread(
        target=run_notify,
        args=(
            client,
            recorder,
            pool,
            profile.soak_duration,
            profile.soak_ws,
            max(1, profile.event_rate // 5),
            profile.file_size_kb,
            ws_path,
            "soak-notify",
        ),
        daemon=True,
    )
    notify_thread.start()
    run_mixed_load(client, recorder, pool, "soak-a", half, profile.soak_http, profile.file_size_kb)
    if restart_command:
        start = time.perf_counter()
        status = 0
        error = None
        try:
            subprocess.check_call(restart_command, shell=True)
        except subprocess.CalledProcessError as err:
            status = err.returncode
            error = repr(err)
        recorder.record("soak", "restart_command", status, (time.perf_counter() - start) * 1000.0, status == 0, error=error)
        wait_for_ready(client, recorder, "soak", 120)
    run_mixed_load(client, recorder, pool, "soak-b", profile.soak_duration - half, profile.soak_http, profile.file_size_kb)
    notify_thread.join()


def wait_for_ready(client: HttpClient, recorder: Recorder, scenario: str, timeout: int) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok, _, _ = record_request(recorder, scenario, "status", client, "GET", "/status.php")
        if ok:
            return
        time.sleep(1)


def write_summary(
    args: argparse.Namespace,
    recorder: Recorder,
    started_at: float,
    report_dir: Path,
    db_path: Optional[str],
) -> int:
    summary = recorder.summary()
    total = sum(item["count"] for item in summary.values())
    failures = list(recorder.failures)
    failed = bool(failures)
    threshold_rows = []
    for op, limit in THRESHOLDS_MS.items():
        if op in summary and summary[op]["count"] > 0:
            p95 = summary[op]["p95_ms"]
            passed = p95 <= limit
            threshold_rows.append((op, p95, limit, passed))
            failed = failed or not passed
    five_xx = 0
    for key, count in recorder.statuses.items():
        status = key.rsplit(":", 1)[-1]
        if status.isdigit() and int(status) >= 500:
            five_xx += count
    five_xx_rate = (five_xx / total) if total else 0.0
    if five_xx_rate >= 0.001:
        failed = True

    run_info = {
        "scenario": args.scenario,
        "profile": args.profile,
        "base_url": args.base_url,
        "user": args.user,
        "started_at": started_at,
        "finished_at": time.time(),
        "duration_secs": round(time.time() - started_at, 3),
        "total_events": total,
        "failure_count": len(failures),
        "five_xx_count": five_xx,
        "five_xx_rate": five_xx_rate,
        "db_path": db_path,
        "summary": summary,
        "thresholds": [
            {"op": op, "p95_ms": p95, "limit_ms": limit, "passed": passed}
            for op, p95, limit, passed in threshold_rows
        ],
        "passed": not failed,
    }
    (report_dir / "run.json").write_text(json.dumps(run_info, indent=2, sort_keys=True), encoding="utf-8")
    with (report_dir / "failures.log").open("w", encoding="utf-8") as file:
        for failure in failures:
            file.write(json.dumps(failure, sort_keys=True) + "\n")
    with (report_dir / "summary.md").open("w", encoding="utf-8") as file:
        file.write("# Gono Cloud Performance Report\n\n")
        file.write(f"- Scenario: `{args.scenario}`\n")
        file.write(f"- Profile: `{args.profile}`\n")
        file.write(f"- Base URL: `{args.base_url}`\n")
        file.write(f"- User: `{args.user}`\n")
        file.write(f"- Duration seconds: `{run_info['duration_secs']}`\n")
        file.write(f"- Total events: `{total}`\n")
        file.write(f"- Failures: `{len(failures)}`\n")
        file.write(f"- 5xx rate: `{five_xx_rate:.6f}`\n")
        file.write(f"- Result: `{'PASS' if not failed else 'FAIL'}`\n\n")
        file.write("## Operation Latency\n\n")
        file.write("| operation | count | p50 ms | p95 ms | p99 ms | max ms |\n")
        file.write("|---|---:|---:|---:|---:|---:|\n")
        for op in sorted(summary):
            row = summary[op]
            file.write(
                f"| `{op}` | {row['count']} | {row['p50_ms']} | {row['p95_ms']} | {row['p99_ms']} | {row['max_ms']} |\n"
            )
        file.write("\n## Thresholds\n\n")
        file.write("| operation | p95 ms | limit ms | result |\n")
        file.write("|---|---:|---:|---|\n")
        for op, p95, limit, passed in threshold_rows:
            file.write(f"| `{op}` | {p95} | {limit} | {'PASS' if passed else 'FAIL'} |\n")
    return 0 if not failed else 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Gono Cloud local performance scenarios.")
    parser.add_argument(
        "scenario",
        choices=["baseline", "mixed", "chunking", "notify", "admin", "spike", "soak", "all"],
    )
    parser.add_argument("--profile", choices=sorted(PROFILES), default=os.environ.get("GONO_PERF_PROFILE", "medium"))
    parser.add_argument("--base-url", default=os.environ.get("GONO_PERF_BASE_URL", "http://127.0.0.1:16102"))
    parser.add_argument("--user", default=os.environ.get("GONO_PERF_USER", "gono"))
    parser.add_argument("--report-dir", default=os.environ.get("GONO_PERF_REPORT_DIR", "target/perf-reports/manual"))
    parser.add_argument("--config", default=os.environ.get("GONO_PERF_CONFIG"))
    parser.add_argument("--pid", type=int, default=int(os.environ["GONO_PERF_PID"]) if os.environ.get("GONO_PERF_PID") else None)
    parser.add_argument("--duration", type=int)
    parser.add_argument("--concurrency", type=int)
    parser.add_argument("--seed-files", type=int)
    parser.add_argument("--seed-concurrency", type=int)
    parser.add_argument("--file-size-kb", type=int)
    parser.add_argument("--chunk-file-mb", type=int)
    parser.add_argument("--chunk-piece-mb", type=int)
    parser.add_argument("--chunk-concurrency", type=int)
    parser.add_argument("--ws-connections", type=int)
    parser.add_argument("--event-rate", type=int)
    parser.add_argument("--sample-interval", type=int, default=5)
    parser.add_argument("--metrics-interval", type=int, default=30)
    parser.add_argument("--ws-path", default=os.environ.get("GONO_PERF_WS_PATH", "/push/ws"))
    parser.add_argument("--restart-command", default=os.environ.get("GONO_PERF_RESTART_COMMAND"))
    parser.add_argument("--skip-preseed", action="store_true")
    parser.add_argument("--insecure-tls", action="store_true", default=True)
    args = parser.parse_args()
    password = os.environ.get("GONO_PERF_PASSWORD")
    if not password:
        parser.error("GONO_PERF_PASSWORD is required")
    args.password = password
    return args


def override_profile(profile: Profile, args: argparse.Namespace) -> Profile:
    values = profile.__dict__.copy()
    if args.duration is not None:
        for key in [
            "baseline_duration",
            "mixed_duration",
            "chunk_duration",
            "notify_duration",
            "admin_duration",
            "spike_duration",
            "soak_duration",
        ]:
            values[key] = args.duration
    mapping = {
        "concurrency": "mixed_concurrency",
        "seed_files": "seed_files",
        "seed_concurrency": "seed_concurrency",
        "file_size_kb": "file_size_kb",
        "chunk_file_mb": "chunk_file_mb",
        "chunk_piece_mb": "chunk_piece_mb",
        "chunk_concurrency": "chunk_concurrency",
        "ws_connections": "ws_connections",
        "event_rate": "event_rate",
    }
    for arg_name, key in mapping.items():
        value = getattr(args, arg_name)
        if value is not None:
            values[key] = value
    return Profile(**values)


def main() -> int:
    args = parse_args()
    profile = override_profile(PROFILES[args.profile], args)
    started_at = time.time()
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    recorder = Recorder(report_dir)
    db_path = load_db_path(args.config)
    client = HttpClient(args.base_url, args.user, args.password, insecure_tls=args.insecure_tls)
    run_id = re.sub(r"[^A-Za-z0-9_.-]", "-", time.strftime("%Y%m%d-%H%M%S"))
    pool = PathPool(f"/remote.php/dav/perf-{run_id}")
    stop = StopFlag()
    sampler = Sampler(
        client,
        recorder,
        stop,
        args.sample_interval,
        args.metrics_interval,
        args.pid,
        db_path,
    )
    sampler.start()
    try:
        record_request(recorder, "preflight", "status", client, "GET", "/status.php")
        auth_ok, _, _ = record_request(
            recorder,
            "preflight",
            "auth_user",
            client,
            "GET",
            "/ocs/v2.php/cloud/user",
            expected={200},
        )
        if not auth_ok:
            print(
                "authenticated preflight failed; check GONO_PERF_USER and GONO_PERF_PASSWORD",
                file=sys.stderr,
            )
            return write_summary(args, recorder, started_at, report_dir, db_path)
        ensure_perf_dirs(client, recorder, pool)
        if not args.skip_preseed and args.scenario in {"mixed", "chunking", "notify", "admin", "spike", "soak", "all"}:
            preseed_files(
                client,
                recorder,
                pool,
                profile.seed_files,
                profile.seed_concurrency,
                profile.file_size_kb,
            )
        scenarios = [
            "baseline",
            "mixed",
            "chunking",
            "notify",
            "admin",
            "spike",
            "soak",
        ] if args.scenario == "all" else [args.scenario]
        for scenario in scenarios:
            if scenario == "baseline":
                run_baseline(client, recorder, pool, profile)
            elif scenario == "mixed":
                run_mixed_load(client, recorder, pool, "mixed", profile.mixed_duration, profile.mixed_concurrency, profile.file_size_kb)
            elif scenario == "chunking":
                run_chunking(client, recorder, pool, profile.chunk_duration, profile.chunk_concurrency, profile.chunk_file_mb, profile.chunk_piece_mb)
            elif scenario == "notify":
                run_notify(client, recorder, pool, profile.notify_duration, profile.ws_connections, profile.event_rate, profile.file_size_kb, args.ws_path)
            elif scenario == "admin":
                run_admin_read(client, recorder, profile.admin_duration, profile.admin_concurrency)
            elif scenario == "spike":
                run_spike(client, recorder, pool, profile, args.ws_path)
            elif scenario == "soak":
                run_soak(client, recorder, pool, profile, args.restart_command, args.ws_path)
    finally:
        stop.stop()
        sampler.join()
        recorder.close()
    return write_summary(args, recorder, started_at, report_dir, db_path)


if __name__ == "__main__":
    sys.exit(main())
