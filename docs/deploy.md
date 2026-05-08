# Deploy Gono One

This document describes the production-oriented install path. The detailed project plan lives in
`plan/07-deploy.md`.

## One-Line Install

Publish `scripts/install.sh` at `https://run.gono.one`, then install on Debian, Ubuntu, or
CentOS/RHEL-compatible systems with:

```sh
bash <(curl -sL https://run.gono.one)
```

The installer:

- downloads a Linux `x86_64` or `aarch64` release artifact from `https://run.gono.one/releases`;
- installs the binary at `/opt/gono-one/bin/gono-one`;
- creates the `gono-one` system user;
- writes `/etc/gono-one/config.toml` if it does not already exist;
- creates `/var/lib/gono-one` for SQLite, files, uploads, and xattrs;
- installs and starts `gono-one.service`.

Default service layout:

```text
/opt/gono-one/bin/gono-one
/etc/gono-one/config.toml
/var/lib/gono-one/
/etc/systemd/system/gono-one.service
```

## Release Artifacts

For the default installer path, publish these files:

```text
https://run.gono.one/releases/latest/gono-one-linux-x86_64.tar.gz
https://run.gono.one/releases/latest/gono-one-linux-aarch64.tar.gz
```

Each archive must contain an executable named `gono-one`. You can also override the artifact URL:

```sh
GONO_ONE_BIN_URL=https://example.com/gono-one-linux-x86_64.tar.gz bash <(curl -sL https://run.gono.one)
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

Use a custom domain or port with:

```sh
GONO_ONE_DOMAIN=files.example.com bash <(curl -sL https://run.gono.one)
GONO_ONE_BASE_URL=https://files.example.com GONO_ONE_BIND=127.0.0.1:18080 bash <(curl -sL https://run.gono.one)
```

## First Password

The service bootstraps user `gono` and prints the generated app password only on the first startup.
Save it immediately:

```sh
journalctl -u gono-one --no-pager \
  | sed -n 's/.*Generated app password for gono: //p' \
  | tail -n 1
```

Normal restarts preserve the existing Argon2id hash and do not print or regenerate the password.

## Acceptance Checks

Before calling the deployment complete:

- run `cargo check`, `cargo test`, and `scripts/compat-smoke.sh`;
- run `RUN_LITMUS=1 scripts/compat-smoke.sh` before release;
- connect with Nextcloud Desktop using the service root URL, such as `https://gono.one`;
- verify upload, download, rename, copy, delete, large chunked upload, and restart behavior;
- confirm `/metrics` requires Basic Auth and logs are collected in the expected format.

## Operational Notes

- Back up SQLite, `data/files`, and xattrs from the same point in time.
- Keep `data/files` and `data/uploads` on the same partition.
- Current WebDAV locks are in memory; a restart drops locks. Document this for MVP deployments.
- Plan follow-up consistency tooling for orphan `file_id` records, dead props, missing xattrs, and
  `change_log` retention.
