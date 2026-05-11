# Deploy Gono Cloud

This document describes the production-oriented install path. The detailed project plan lives in
`plan/07-deploy.md`.

## One-Line Install

Publish `scripts/install.sh` at `https://run.gono.cloud`, then install on macOS, Debian, Ubuntu, or
CentOS/RHEL-compatible systems with:

```sh
bash <(curl -sL https://run.gono.cloud)
```

Running without arguments starts the interactive menu. Choose `1` to install or upgrade, `2` to
restart, `3` to uninstall, `4` to show status, `5` to follow logs, `6` for help, or `0` to exit.

To make that command work in production, `https://run.gono.cloud` must serve the raw contents of
`scripts/install.sh` at the origin path `/`. Redirects are fine as long as `curl -sL` reaches the
script. Remote installs download the latest release archive from GitHub Releases.

The installer:

- detects macOS or Linux and downloads the matching `x86_64`, `aarch64`, or Linux 32-bit ARM
  release artifact from GitHub Releases;
- when run from a checked-out repository, automatically prefers a local release binary/build
  instead of downloading a release artifact;
- installs the binary at `/opt/gono-cloud/bin/gono-cloud`;
- on Linux, creates the `gono-cloud` system user and installs `gono-cloud.service` through systemd;
- on macOS, installs a LaunchDaemon named `cloud.gono.gono-cloud`;
- asks for first-install settings and writes the platform config file if it does not already exist;
- creates the platform data directory for SQLite, files, uploads, and xattrs.

Web admin is disabled by default. During first install, the menu asks whether to enable the built-in
`/admin` management page. When enabled by the installer, `gono` is granted admin access by default.
Configured admin users that do not exist yet are created at startup, and their one-time app passwords
are written to the service log. Existing config files are preserved by the installer during upgrades,
so add or edit `[admin]` manually when enabling admin on an existing installation. When exposing
`/admin` through a reverse proxy, consider adding an IP allowlist or another network-level access
control in front of it.

Runtime configuration is read from `config.toml` at startup. The `/admin/settings` page is a
read-only view of the effective config; it does not save changes. To change `server.base_url`,
`auth.realm`, sync retention, notify push options, or admin access, edit `config.toml` and restart
the service.

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
/Library/Application Support/Gono Cloud/data/users/<username>/files
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

Each archive must contain an executable named `gono-cloud`. The interactive installer does not
accept command-line overrides; publish the latest-style assets above before using the remote install
entrypoint.

Build a release archive for the current host with:

```sh
scripts/package-release.sh
```

The script writes the installer-facing latest-style archive to `dist/`, plus a `.sha256` sidecar.
For example:

```text
dist/gono-cloud-linux-x86_64.tar.gz
dist/gono-cloud-linux-x86_64.tar.gz.sha256
```

Cross-builds can pass Cargo and release target names explicitly:

```sh
GONO_CLOUD_CARGO_TARGET=aarch64-unknown-linux-gnu \
GONO_CLOUD_RELEASE_TARGET=linux-aarch64 \
scripts/package-release.sh
```

On `v*` tag builds, GitHub Actions packages native Linux/macOS artifacts for `x86_64` and
`aarch64`, then creates or updates the matching GitHub Release with latest-style `.tar.gz` files and
`.sha256` sidecars. Re-running a release job also removes old versioned asset names from the same
tag. Manual GitHub Actions runs package the same artifacts and can also run the separate litmus
compatibility job without creating a GitHub Release. Ordinary pushes and pull requests do not run
the Rust check/smoke job in CI.

The installer uses latest GitHub Release URLs:

```text
https://github.com/Gono-Dev/cloud.server/releases/latest/download/gono-cloud-linux-x86_64.tar.gz
```

If a `.sha256` sidecar is present, the installer downloads it and verifies the archive
automatically.

Create the GitHub Release from a clean `main` branch with:

```sh
git push origin main
git tag -a v0.1.3 -m "v0.1.3"
git push origin v0.1.3
```

Linux `armv7` and `armv6` remain installer-supported artifact names, but they require a separate
self-hosted or cross-build release step before they are available from GitHub Releases.

## Local Install

From a checked-out repository, the installer can be used directly:

```sh
scripts/install.sh
```

This opens the same interactive menu as the remote installer. Choose `1` to install or upgrade. If
the script needs elevated privileges, it builds or locates the local release binary first when
possible, then re-runs the selected action through `sudo`. Local repository installs prefer
`target/release/gono-cloud` or a fresh release build; remote installs continue to use release
artifacts.

The installer no longer accepts command-line subcommands or options. Use the menu for service
management: restart, uninstall, status, and logs. User and app-password management is available from
the Web Admin UI after admin is enabled.

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
make sure the proxy keeps the upgrade headers. A complete Nginx reference configuration is available
in [docs/nginx.md](nginx.md):

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

On a first install, choose `1` in the interactive menu and enter the custom domain, base URL, and
bind address when prompted. Existing installations preserve `config.toml`; edit the config file for
startup-only values such as bind address, then restart from the menu.

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
- for local release validation, run the performance gate described in [docs/performance.md](performance.md);
- connect with Gono Cloud Desktop using the service root URL, such as `https://gono.cloud`;
- verify upload, download, rename, copy, delete, large chunked upload, and restart behavior;
- confirm capabilities advertise `notify_push` and `/push/ws` accepts WebSocket login;
- confirm `/metrics` requires Basic Auth and logs are collected in the expected format.

## Operational Notes

- Back up SQLite, `data/users`, and xattrs from the same point in time.
- `OC-FileId` is stable across ordinary restarts because the instance suffix is stored in SQLite
  settings as `instance.id`. Preserve the database when restoring file data, otherwise clients will
  see a new instance identity.
- Run `GONE_CLOUD_CONFIG=/etc/gono-cloud/config.toml gono-cloud consistency-check` after restores or
  manual filesystem maintenance. It is read-only and reports SQLite/file/xattr mismatches, orphan
  `file_id` rows, and orphan dead props.
- Run `GONE_CLOUD_CONFIG=/etc/gono-cloud/config.toml gono-cloud consistency-repair` first to preview safe
  fixes. Only run `gono-cloud consistency-repair --apply` after a backup; it can create missing
  `file_ids`, rewrite missing or stale xattrs/cache, and remove orphan `file_ids`/dead props.
- There is no `metadata-prune` command in the current binary. Metadata cleanup is handled by
  `consistency-check`, `consistency-repair`, normal write finalization, and `change_log` retention.
- Configure `[sync] change_log_retention_days` and `change_log_min_entries` for the deployment's
  sync history window. Startup prunes every enabled local user, and writes also prune the current
  owner. Rows older than the retention window are pruned only when they fall outside the minimum
  retained row count; clients with a token older than the retained floor receive
  `DAV:valid-sync-token` and must do a full resync.
- Keep `data/users/gono/files` and `data/uploads` on the same partition. Startup rejects split partitions,
  validates the xattr namespace, and probes xattr writes before accepting traffic.
- Configure `[storage] upload_min_free_bytes` to reserve disk space before accepting uploads. The
  default is 1 GiB; `[storage] upload_min_free_percent` can add a percentage-based reserve, and the
  stricter threshold wins. Uploads that would cross the reserve return WebDAV
  `507 Insufficient Storage`.
- WebDAV locks are persisted in SQLite. A normal restart preserves active locks until they expire
  or the client sends `UNLOCK`; run a single service instance for now so lock conflict checks stay
  serialized through the process-local lock guard shared by principal scope.
- WebDAV `SEARCH` is disabled and returns `501 Not Implemented`; use `PROPFIND`,
  `sync-collection REPORT`, or `oc:filter-files REPORT` instead. Favorite `oc:filter-files` still
  uses SQLite indexed candidates before touching the filesystem. External deletes are filtered out
  by path canonicalization and metadata refresh; run consistency repair after manual filesystem
  maintenance to remove stale index rows.
- Linux service status: `systemctl status gono-cloud`.
- macOS service status: `launchctl print system/cloud.gono.gono-cloud`.
