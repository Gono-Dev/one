# Deploy Gono Cloud

This document describes the production-oriented install path. The detailed project plan lives in
`plan/07-deploy.md`.

## One-Line Install

Publish `scripts/install.sh` at `https://run.gono.cloud`, then install on macOS, Debian, Ubuntu, or
CentOS/RHEL-compatible systems with:

```sh
bash <(curl -sL https://run.gono.cloud)
```

To make that command work in production, `https://run.gono.cloud` must serve the raw contents of
`scripts/install.sh` at the origin path `/`. Redirects are fine as long as `curl -sL` reaches the
script. Release archives are downloaded from the GitHub Release by default, or users can pass
`GONO_CLOUD_BIN_URL`/`--bin-url`.

The installer:

- detects macOS or Linux and downloads the matching `x86_64`, `aarch64`, or Linux 32-bit ARM
  release artifact from GitHub Releases;
- when run from a checked-out repository, defaults to building and installing the local binary
  instead of downloading a release artifact;
- installs the binary at `/opt/gono-cloud/bin/gono-cloud`;
- on Linux, creates the `gono-cloud` system user and installs `gono-cloud.service` through systemd;
- on macOS, installs a LaunchDaemon named `cloud.gono.gono-cloud`;
- writes the platform config file if it does not already exist;
- creates the platform data directory for SQLite, files, uploads, and xattrs.

Default Linux layout:

```text
/opt/gono-cloud/bin/gono-cloud
/etc/gono-cloud/config.toml
/var/lib/gono-cloud/
/etc/systemd/system/gono-cloud.service
```

Default macOS layout:

```text
/opt/gono-cloud/bin/gono-cloud
/Library/Application Support/Gono Cloud/config.toml
/Library/Application Support/Gono Cloud/gono-cloud.db
/Library/Application Support/Gono Cloud/data/files
/Library/Application Support/Gono Cloud/data/uploads
/Library/LaunchDaemons/cloud.gono.gono-cloud.plist
/Library/Logs/Gono Cloud/
```

## Release Artifacts

For the default installer path, publish these files:

```text
https://github.com/Gono-Dev/cloud.server/releases/latest/download/gono-cloud-linux-x86_64.tar.gz
https://github.com/Gono-Dev/cloud.server/releases/latest/download/gono-cloud-linux-aarch64.tar.gz
https://github.com/Gono-Dev/cloud.server/releases/latest/download/gono-cloud-linux-armv7.tar.gz
https://github.com/Gono-Dev/cloud.server/releases/latest/download/gono-cloud-linux-armv6.tar.gz
https://github.com/Gono-Dev/cloud.server/releases/latest/download/gono-cloud-macos-x86_64.tar.gz
https://github.com/Gono-Dev/cloud.server/releases/latest/download/gono-cloud-macos-aarch64.tar.gz
```

ARM mapping:

| Platform | `uname -m` examples | Artifact arch |
|----------|---------------------|---------------|
| macOS Apple Silicon | `arm64` | `macos-aarch64` |
| Debian/Ubuntu/CentOS/RHEL ARM64 | `aarch64`, `arm64` | `linux-aarch64` |
| Debian/Ubuntu 32-bit ARMv7 | `armv8l`, `armv7l`, `armhf` | `linux-armv7` |
| Debian/Ubuntu 32-bit ARMv6 | `armv6l` | `linux-armv6` |

Set `GONO_CLOUD_ARCH` to override architecture detection on unusual systems:

```sh
GONO_CLOUD_ARCH=aarch64 bash <(curl -sL https://run.gono.cloud)
GONO_CLOUD_ARCH=armv7 bash <(curl -sL https://run.gono.cloud)
```

Each archive must contain an executable named `gono-cloud`. You can also override the artifact URL:

```sh
GONO_CLOUD_BIN_URL=https://example.com/gono-cloud-linux-x86_64.tar.gz bash <(curl -sL https://run.gono.cloud)
```

Build a release archive for the current host with:

```sh
scripts/package-release.sh
```

The script writes both latest-style and versioned names to `dist/`, plus `.sha256` sidecars. For
example:

```text
dist/gono-cloud-linux-x86_64.tar.gz
dist/gono-cloud-0.1.0-linux-x86_64.tar.gz
dist/gono-cloud-linux-x86_64.tar.gz.sha256
dist/gono-cloud-0.1.0-linux-x86_64.tar.gz.sha256
```

Cross-builds can pass Cargo and installer target names explicitly:

```sh
GONO_CLOUD_CARGO_TARGET=aarch64-unknown-linux-gnu \
GONO_CLOUD_RELEASE_TARGET=linux-aarch64 \
scripts/package-release.sh
```

On `v*` tag builds, GitHub Actions packages native Linux/macOS artifacts for `x86_64` and
`aarch64`, then creates or updates the matching GitHub Release with the `.tar.gz` files and
`.sha256` sidecars. Manual GitHub Actions runs package the same artifacts and can also run the
separate litmus compatibility job without creating a GitHub Release. Ordinary pushes and pull
requests do not run the Rust check/smoke job in CI.

The installer uses GitHub Release URLs by default:

```text
latest:
https://github.com/Gono-Dev/cloud.server/releases/latest/download/gono-cloud-linux-x86_64.tar.gz

versioned:
https://github.com/Gono-Dev/cloud.server/releases/download/v0.1.0/gono-cloud-0.1.0-linux-x86_64.tar.gz
```

For versioned installs, both `--version 0.1.0` and `--version v0.1.0` resolve to the tag
`v0.1.0` and the asset name `gono-cloud-0.1.0-<target>.tar.gz`. If a `.sha256` sidecar is present,
the installer downloads it and verifies the archive automatically.

Create the first GitHub Release from a clean `main` branch with:

```sh
git push origin main
git tag -a v0.1.0 -m "v0.1.0"
git push origin v0.1.0
```

Linux `armv7` and `armv6` remain installer-supported artifact names, but they require a separate
self-hosted or cross-build release step before they are available from GitHub Releases.

## Local Install

From a checked-out repository, the installer can be used directly:

```sh
scripts/install.sh
```

If it needs elevated privileges, it first builds the local binary as the current user and then
re-runs the same local script through `sudo`. This avoids downloading `https://run.gono.cloud` while
developing locally.

Useful local overrides:

```sh
GONO_CLOUD_BUILD_PROFILE=debug scripts/install.sh
GONO_CLOUD_BIN=/absolute/path/to/gono-cloud scripts/install.sh
GONO_CLOUD_LOCAL_BUILD=0 GONO_CLOUD_BIN=target/release/gono-cloud scripts/install.sh
GONO_CLOUD_INSTALL_SOURCE=release scripts/install.sh
```

`GONO_CLOUD_INSTALL_SOURCE=auto` is the default: local repository installs use the local binary, while
`bash <(curl -sL https://run.gono.cloud)` continues to use release artifacts.

The installer also accepts command-line options for the common environment variables:

```sh
scripts/install.sh --help
scripts/install.sh --debug
scripts/install.sh --bin target/release/gono-cloud
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
base_url = https://gono.cloud
```

Public HTTPS should terminate at a reverse proxy and forward to the local service:

```caddyfile
gono.cloud {
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
GONO_CLOUD_DOMAIN=files.example.com bash <(curl -sL https://run.gono.cloud)
GONO_CLOUD_BASE_URL=https://files.example.com GONO_CLOUD_BIND=127.0.0.1:18080 bash <(curl -sL https://run.gono.cloud)
```

## First Password

The service bootstraps user `gono` and prints the generated app password only on the first startup.
Save it immediately:

Linux:

```sh
journalctl -u gono-cloud --no-pager \
  | sed -n 's/.*Generated app password for gono: //p' \
  | tail -n 1
```

macOS:

```sh
sed -n 's/.*Generated app password for gono: //p' \
  "/Library/Logs/Gono Cloud/stdout.log" \
  "/Library/Logs/Gono Cloud/stderr.log" \
  | tail -n 1
```

Normal restarts preserve the existing Argon2id hash and do not print or regenerate the password.

## Acceptance Checks

Before calling the deployment complete:

- run `cargo check`, `cargo test`, and `scripts/compat-smoke.sh`;
- optionally run `RUN_LITMUS=1 scripts/compat-smoke.sh` or the manual litmus workflow before
  release;
- connect with Nextcloud Desktop using the service root URL, such as `https://gono.cloud`;
- verify upload, download, rename, copy, delete, large chunked upload, and restart behavior;
- confirm capabilities advertise `notify_push` and `/push/ws` accepts WebSocket login;
- confirm `/metrics` requires Basic Auth and logs are collected in the expected format.

## Operational Notes

- Back up SQLite, `data/files`, and xattrs from the same point in time.
- Run `NC_DAV_CONFIG=/etc/gono-cloud/config.toml gono-cloud consistency-check` after restores or
  manual filesystem maintenance. It is read-only and reports SQLite/file/xattr mismatches, orphan
  `file_id` rows, and orphan dead props.
- Run `NC_DAV_CONFIG=/etc/gono-cloud/config.toml gono-cloud consistency-repair` first to preview safe
  fixes. Only run `gono-cloud consistency-repair --apply` after a backup; it can create missing
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
- Linux service status: `systemctl status gono-cloud`.
- macOS service status: `launchctl print system/cloud.gono.gono-cloud`.
