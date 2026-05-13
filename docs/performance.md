# Performance Gate

This page describes the local medium-system performance runner for an already installed Gono Cloud
service.

## Quick Start

Run a short local validation:

```sh
GONO_PERF_PROFILE=quick \
GONO_PERF_BASE_URL=http://127.0.0.1:16102 \
GONO_PERF_USER=gono \
GONO_PERF_PASSWORD='app-password-here' \
scripts/perf-gate.sh baseline
```

Run the medium release profile:

```sh
GONO_PERF_PROFILE=medium \
GONO_PERF_BASE_URL=http://127.0.0.1:16102 \
GONO_PERF_USER=gono \
GONO_PERF_PASSWORD='app-password-here' \
scripts/perf-gate.sh all
```

If the installed config is readable, the wrapper auto-detects:

- macOS config: `/Library/Application Support/Gono Cloud/config.toml`
- Linux config: `/etc/gono-cloud/config.toml`
- installed binary: `/opt/gono-cloud/bin/gono-cloud`

To create a temporary full-scope app password for the target user through the local binary:

```sh
sudo GONO_PERF_CREATE_PASSWORD=1 GONO_PERF_PROFILE=quick scripts/perf-gate.sh baseline
```

## Scenarios

- `baseline`: sequential status, capabilities, PROPFIND, GET, PUT, COPY, MOVE, DELETE, sync REPORT,
  and one chunking v2 upload.
- `mixed`: concurrent WebDAV mix with PROPFIND/HEAD/GET/PUT/MOVE/COPY/DELETE/sync REPORT.
- `chunking`: concurrent Nextcloud chunking v2 uploads.
- `notify`: WebSocket Notify Push connections plus file events.
- `admin`: read-only `/gono-admin/users` and `/gono-admin/settings` interference load.
- `spike`: ramp to high HTTP and WebSocket pressure, then verify resources settle.
- `soak`: long mixed load; optionally pass `GONO_PERF_RESTART_COMMAND` to restart mid-run.
- `all`: runs every scenario in order.

## Current Hot Paths

The service keeps WebDAV metadata in xattrs and uses SQLite as an index/read-through cache. Current
query behavior is:

- Successful Basic Auth checks are cached in memory for a short TTL, keyed by a hash of the
  username/password material. This avoids running Argon2 on every WebDAV request during large
  directory uploads while still revalidating after the TTL and clearing affected entries when admin
  user/password state changes.
- `OC-FileId` uses the SQLite `file_ids.id` plus the persisted `settings["instance.id"]` suffix.
- WebDAV `SEARCH` is disabled and returns a friendly `501 Not Implemented` response.
- `REPORT oc:filter-files` for favorites uses the `file_ids(owner, favorite, rel_path)` runtime
  index first, then verifies each candidate through `safe_existing_path`, scope filtering, and a
  metadata refresh.
- `change_log` pruning runs per enabled local user at startup and is throttled after writes so large
  small-file uploads do not run the retention query on every file.
- Notify Push file events are coalesced in a short per-user window before broadcasting to WebSocket
  listeners. Per-file queue/send logs are `debug` level to avoid log I/O becoming a hot path.
- WebDAV locks are stored in SQLite and guarded by a process-local gate shared by principal scope.
- Uploads run a `statvfs`-style free-space preflight before ordinary `PUT`, chunking `MKCOL`, chunk
  writes, and final `MOVE .file`; failures return WebDAV `507 Insufficient Storage`.

## Medium Defaults

- 20,000 seeded hot files.
- 150 concurrent mixed WebDAV workers for 45 minutes.
- 20 concurrent chunking uploads for 30 minutes, 256 MB per upload.
- 1,000 Notify Push WebSocket connections for 45 minutes.
- 300 HTTP workers and 2,000 WebSocket connections during spike.
- 8 hour soak with 100 HTTP workers and 1,000 WebSocket connections.

The Python runner raises its own open-file limit when the platform allows it. The medium `all`
profile currently targets at least 4,096 open files so the spike phase can hold WebSocket
connections, HTTP workers, metrics sampling, and report files at the same time. If the runner prints
an open-file warning, start the shell with a higher limit before running the gate:

```sh
ulimit -n 4096
```

If WebSocket scenarios fail from the service side with too many open files, raise the service
manager's open-file limit as well before starting Gono Cloud.

Override any size or duration from the command line, for example:

```sh
GONO_PERF_PROFILE=medium GONO_PERF_PASSWORD='app-password-here' \
scripts/perf-gate.sh mixed --duration 300 --concurrency 50 --seed-files 1000
```

If `[notify_push].path` is not `/push`, pass the WebSocket path:

```sh
GONO_PERF_WS_PATH=/custom/ws GONO_PERF_PASSWORD='app-password-here' scripts/perf-gate.sh notify
```

## Reports

Reports are written to `target/perf-reports/<timestamp>/` by default:

- `summary.md`
- `run.json`
- `raw/events.jsonl`
- `metrics.prom`
- `system.csv`
- `failures.log`

The process exits non-zero when request failures occur or when configured p95 thresholds are missed.
