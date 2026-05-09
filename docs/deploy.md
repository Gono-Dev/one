# Deploy Gono One

This document describes the production-oriented install path. The detailed project plan lives in
`plan/07-deploy.md`.

## One-Line Install

Publish `scripts/install.sh` at `https://run.gono.one`, then install on macOS, Debian, Ubuntu, or
CentOS/RHEL-compatible systems with:

```sh
bash <(curl -sL https://run.gono.one)
```

The installer:

- detects macOS or Linux and downloads the matching `x86_64`, `aarch64`, or Linux 32-bit ARM
  release artifact from `https://run.gono.one/releases`;
- when run from a checked-out repository, defaults to building and installing the local binary
  instead of downloading a release artifact;
- installs the binary at `/opt/gono-one/bin/gono-one`;
- on Linux, creates the `gono-one` system user and installs `gono-one.service` through systemd;
- on macOS, installs a LaunchDaemon named `one.gono.gono-one`;
- writes the platform config file if it does not already exist;
- creates the platform data directory for SQLite, files, uploads, and xattrs.

Default Linux layout:

```text
/opt/gono-one/bin/gono-one
/etc/gono-one/config.toml
/var/lib/gono-one/
/etc/systemd/system/gono-one.service
```

Default macOS layout:

```text
/opt/gono-one/bin/gono-one
/Library/Application Support/Gono One/config.toml
/Library/Application Support/Gono One/gono-one.db
/Library/Application Support/Gono One/data/files
/Library/Application Support/Gono One/data/uploads
/Library/LaunchDaemons/one.gono.gono-one.plist
/Library/Logs/Gono One/
```

## Release Artifacts

For the default installer path, publish these files:

```text
https://run.gono.one/releases/latest/gono-one-linux-x86_64.tar.gz
https://run.gono.one/releases/latest/gono-one-linux-aarch64.tar.gz
https://run.gono.one/releases/latest/gono-one-linux-armv7.tar.gz
https://run.gono.one/releases/latest/gono-one-linux-armv6.tar.gz
https://run.gono.one/releases/latest/gono-one-macos-x86_64.tar.gz
https://run.gono.one/releases/latest/gono-one-macos-aarch64.tar.gz
```

ARM mapping:

| Platform | `uname -m` examples | Artifact arch |
|----------|---------------------|---------------|
| macOS Apple Silicon | `arm64` | `macos-aarch64` |
| Debian/Ubuntu/CentOS/RHEL ARM64 | `aarch64`, `arm64` | `linux-aarch64` |
| Debian/Ubuntu 32-bit ARMv7 | `armv8l`, `armv7l`, `armhf` | `linux-armv7` |
| Debian/Ubuntu 32-bit ARMv6 | `armv6l` | `linux-armv6` |

Set `GONO_ONE_ARCH` to override architecture detection on unusual systems:

```sh
GONO_ONE_ARCH=aarch64 bash <(curl -sL https://run.gono.one)
GONO_ONE_ARCH=armv7 bash <(curl -sL https://run.gono.one)
```

Each archive must contain an executable named `gono-one`. You can also override the artifact URL:

```sh
GONO_ONE_BIN_URL=https://example.com/gono-one-linux-x86_64.tar.gz bash <(curl -sL https://run.gono.one)
```

Build a release archive for the current host with:

```sh
scripts/package-release.sh
```

The script writes both latest-style and versioned names to `dist/`, plus `.sha256` sidecars. For
example:

```text
dist/gono-one-linux-x86_64.tar.gz
dist/gono-one-0.1.0-linux-x86_64.tar.gz
dist/gono-one-linux-x86_64.tar.gz.sha256
dist/gono-one-0.1.0-linux-x86_64.tar.gz.sha256
```

Cross-builds can pass Cargo and installer target names explicitly:

```sh
GONO_ONE_CARGO_TARGET=aarch64-unknown-linux-gnu \
GONO_ONE_RELEASE_TARGET=linux-aarch64 \
scripts/package-release.sh
```

On tag builds or manual GitHub Actions runs, CI also uploads native Linux/macOS package artifacts.
After verifying them, mirror the latest-style files under `https://run.gono.one/releases/latest/`
and the versioned files under `https://run.gono.one/releases/<version>/`.

## Local Install

From a checked-out repository, the installer can be used directly:

```sh
scripts/install.sh
```

If it needs elevated privileges, it first builds the local binary as the current user and then
re-runs the same local script through `sudo`. This avoids downloading `https://run.gono.one` while
developing locally.

Useful local overrides:

```sh
GONO_ONE_BUILD_PROFILE=debug scripts/install.sh
GONO_ONE_BIN=/absolute/path/to/gono-one scripts/install.sh
GONO_ONE_LOCAL_BUILD=0 GONO_ONE_BIN=target/release/gono-one scripts/install.sh
GONO_ONE_INSTALL_SOURCE=release scripts/install.sh
```

`GONO_ONE_INSTALL_SOURCE=auto` is the default: local repository installs use the local binary, while
`bash <(curl -sL https://run.gono.one)` continues to use release artifacts.

The installer also accepts command-line options for the common environment variables:

```sh
scripts/install.sh --help
scripts/install.sh --debug
scripts/install.sh --bin target/release/gono-one
scripts/install.sh --release --version latest
scripts/install.sh --domain files.example.com --bind 127.0.0.1:18080
```

It can also be used as a lightweight service management script, in the same style as common
one-file installers:

```sh
scripts/install.sh status
scripts/install.sh logs
scripts/install.sh restart
scripts/install.sh uninstall
scripts/install.sh uninstall --purge
```

## Domain And Reverse Proxy

The installer defaults to:

```text
bind = 127.0.0.1:16102
base_url = https://gono.one
```

Public HTTPS should terminate at a reverse proxy and forward to the local service:

```caddyfile
gono.one {
    reverse_proxy 127.0.0.1:16102
}
```

Notify Push uses WebSocket at `/push/ws`. Caddy handles WebSocket upgrade automatically. For Nginx,
make sure the proxy keeps the upgrade headers:

```nginx
location / {
    proxy_pass http://127.0.0.1:16102;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}

location /push/ws {
    proxy_pass http://127.0.0.1:16102;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}
```

Use a custom domain or port with:

```sh
GONO_ONE_DOMAIN=files.example.com bash <(curl -sL https://run.gono.one)
GONO_ONE_BASE_URL=https://files.example.com GONO_ONE_BIND=127.0.0.1:18080 bash <(curl -sL https://run.gono.one)
```

## First Password

The service bootstraps user `gono` and prints the generated app password only on the first startup.
Save it immediately:

Linux:

```sh
journalctl -u gono-one --no-pager \
  | sed -n 's/.*Generated app password for gono: //p' \
  | tail -n 1
```

macOS:

```sh
sed -n 's/.*Generated app password for gono: //p' \
  "/Library/Logs/Gono One/stdout.log" \
  "/Library/Logs/Gono One/stderr.log" \
  | tail -n 1
```

Normal restarts preserve the existing Argon2id hash and do not print or regenerate the password.

## Acceptance Checks

Before calling the deployment complete:

- run `cargo check`, `cargo test`, and `scripts/compat-smoke.sh`;
- run `RUN_LITMUS=1 scripts/compat-smoke.sh` before release;
- connect with Nextcloud Desktop using the service root URL, such as `https://gono.one`;
- verify upload, download, rename, copy, delete, large chunked upload, and restart behavior;
- confirm capabilities advertise `notify_push` and `/push/ws` accepts WebSocket login;
- confirm `/metrics` requires Basic Auth and logs are collected in the expected format.

## Operational Notes

- Back up SQLite, `data/files`, and xattrs from the same point in time.
- Run `NC_DAV_CONFIG=/etc/gono-one/config.toml gono-one consistency-check` after restores or
  manual filesystem maintenance. It is read-only and reports SQLite/file/xattr mismatches, orphan
  `file_id` rows, and orphan dead props.
- Run `NC_DAV_CONFIG=/etc/gono-one/config.toml gono-one consistency-repair` first to preview safe
  fixes. Only run `gono-one consistency-repair --apply` after a backup; it can create missing
  `file_ids`, rewrite missing or stale xattrs/cache, and remove orphan `file_ids`/dead props.
- Configure `[sync] change_log_retention_days` and `change_log_min_entries` for the deployment's
  sync history window. Rows older than the retention window are pruned only when they fall outside
  the minimum retained row count; clients with a token older than the retained floor receive
  `DAV:valid-sync-token` and must do a full resync.
- Keep `data/files` and `data/uploads` on the same partition. Startup rejects split partitions,
  validates the xattr namespace, and probes xattr writes before accepting traffic.
- WebDAV locks are persisted in SQLite. A normal restart preserves active locks until they expire
  or the client sends `UNLOCK`; run a single service instance for now so lock conflict checks stay
  serialized through the process-local lock guard.
- Linux service status: `systemctl status gono-one`.
- macOS service status: `launchctl print system/one.gono.gono-one`.
