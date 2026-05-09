# Compatibility Smoke

Use this page for Phase 5 compatibility checks that need a real HTTP server.

## Built-In Smoke

Run:

```sh
scripts/compat-smoke.sh
```

The script creates a temporary config and data directory, starts `gono-one` over local plain HTTP with `NC_DAV_INSECURE_HTTP=1`, reads the one-time `gono` app password from startup logs, and checks:

- `/status.php`
- capabilities
- Basic Auth failure
- `PROPFIND`
- root path WebDAV compatibility
- `PUT`, `GET`, `COPY`, `MOVE`, `DELETE`
- Nextcloud chunking v2 `MKCOL + PUT chunks + MOVE .file`
- notify_push capabilities and WebSocket authentication
- authenticated `/metrics`

Set `KEEP_SMOKE_DIR=1` to keep the temporary data directory for inspection. Set `NC_DAV_SMOKE_PORT=16102` to force a port.

## CI

GitHub Actions runs `cargo fmt --check`, `cargo check --locked`, `cargo test --locked`, and
`scripts/compat-smoke.sh` on pushes and pull requests. The heavier litmus job runs for tags and
manual workflow dispatch.

## Litmus

Install the external `litmus` WebDAV test binary, then run:

```sh
RUN_LITMUS=1 scripts/compat-smoke.sh
```

The script runs the built-in smoke first and then invokes:

```sh
litmus http://127.0.0.1:<port>/remote.php/dav/ gono <generated-app-password>
```

If `litmus` is not installed, the script exits with a clear error.

## Nextcloud Desktop Smoke

Start the service with a real TLS certificate or with local-only HTTP for development. For the Nextcloud desktop client, use the service root URL:

```text
http://127.0.0.1:<port>
```

WebDAV-only clients can connect directly to either `http://127.0.0.1:<port>/remote.php/dav/` or the root path `http://127.0.0.1:<port>/`. Both paths expose the same storage namespace. Use user `gono` and the generated app password from the first startup log. Check these operations:

- First connection completes.
- Upload a small file.
- Rename the file.
- Copy the file.
- Delete the file.
- Upload a larger file through chunking.
- Stop and restart the server; the old app password still works and is not printed again.
