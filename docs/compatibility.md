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

GitHub Actions no longer runs the Rust smoke checks on ordinary pushes or pull requests. Tag builds
create release artifacts, while the heavier litmus job is kept as a manual workflow dispatch check
so it can report WebDAV edge-case warnings without blocking GitHub Release creation.

## Litmus

Install the external `litmus` WebDAV test binary, then run:

```sh
RUN_LITMUS=1 scripts/compat-smoke.sh
```

If `litmus` is available outside `PATH`, pass the binary explicitly:

```sh
LITMUS=/path/to/litmus RUN_LITMUS=1 scripts/compat-smoke.sh
```

The script runs the built-in smoke first and then invokes:

```sh
litmus http://127.0.0.1:<port>/remote.php/dav/ gono <generated-app-password>
```

If `litmus` is not installed or `LITMUS` points to a non-executable file, the script exits with a clear error.

Current compatibility status with litmus 0.17:

- `basic`, `copymove`, `props`, `locks`, and `http` pass.
- Dead property removals return explicit `404 Not Found` propstat entries.
- Persisted lock discovery preserves the client-supplied owner value.
- The only remaining warning is `delete_fragment`: neon/litmus and the HTTP stack normalize `#fragment` before the request reaches the WebDAV service, so the handler cannot reliably distinguish `/frag/` from `/frag/#ment`. If a lower layer exposes `#` in the request target, `gono-one` rejects it before dispatch.

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
