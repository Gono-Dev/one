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
- `admin`: read-only `/admin/users` and `/admin/settings` interference load.
- `spike`: ramp to high HTTP and WebSocket pressure, then verify resources settle.
- `soak`: long mixed load; optionally pass `GONO_PERF_RESTART_COMMAND` to restart mid-run.
- `all`: runs every scenario in order.

## Medium Defaults

- 20,000 seeded hot files.
- 150 concurrent mixed WebDAV workers for 45 minutes.
- 20 concurrent chunking uploads for 30 minutes, 256 MB per upload.
- 1,000 Notify Push WebSocket connections for 45 minutes.
- 300 HTTP workers and 2,000 WebSocket connections during spike.
- 8 hour soak with 100 HTTP workers and 1,000 WebSocket connections.

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
