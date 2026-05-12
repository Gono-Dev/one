# Nginx Reverse Proxy Reference

This project is usually run as a local service on `127.0.0.1:16102`, with public HTTPS terminated
by a reverse proxy. The installer writes an empty `base_url` by default:

```toml
[server]
bind = "127.0.0.1:16102"
base_url = ""

[notify_push]
enabled = true
path = "/push"
```

For your deployment, set `base_url` to the exact public origin that users open in their clients,
for example `https://files.example.com`. Notify Push capability URLs are built from this value.
If `base_url` is omitted or left empty, Gono Cloud leaves the setting empty and infers an origin at
request time for responses that need absolute URLs. This is convenient for local testing, but
reverse-proxy deployments should set the public origin explicitly.

## Full HTTPS Site

Use this as a starting point for `/etc/nginx/conf.d/gono-cloud.conf` or a Debian/Ubuntu
`sites-available` file. Replace `files.example.com` and the certificate paths.

```nginx
upstream gono_cloud {
    server 127.0.0.1:16102;
    keepalive 32;
}

map $http_upgrade $connection_upgrade {
    default upgrade;
    '' close;
}

server {
    listen 80;
    listen [::]:80;
    server_name files.example.com;

    location /.well-known/acme-challenge/ {
        root /var/www/letsencrypt;
    }

    location / {
        return 301 https://$host$request_uri;
    }
}

server {
    listen 443 ssl;
    listen [::]:443 ssl;
    server_name files.example.com;

    ssl_certificate /etc/letsencrypt/live/files.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/files.example.com/privkey.pem;

    client_max_body_size 0;

    location / {
        proxy_pass http://gono_cloud;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_request_buffering off;
        proxy_buffering off;
        proxy_max_temp_file_size 0;

        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
    }

    location = /push/ws {
        proxy_pass http://gono_cloud;
        proxy_http_version 1.1;

        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_read_timeout 3600s;
        proxy_send_timeout 3600s;
        proxy_buffering off;
    }
}
```

## Service Settings

Recommended local service settings when Nginx terminates HTTPS:

```toml
[server]
bind = "127.0.0.1:16102"
base_url = "https://files.example.com"
```

The packaged service defaults to local plain HTTP through `GONE_CLOUD_INSECURE_HTTP=1`, which is
appropriate only while the bind address remains loopback-only. If you expose the application without
a reverse proxy, provide `cert_file` and `key_file` instead of using insecure HTTP.
Leaving `base_url` empty is supported for development; capabilities responses use the request `Host`
and request protocol headers when available, falling back to a local origin derived from `bind`.
Production deployments should still prefer an explicit `base_url` so generated endpoints are stable.

## Notes

- Keep `proxy_pass http://gono_cloud;` without a path suffix so WebDAV, OCS, metrics, and well-known
  routes reach the Rust service unchanged.
- `client_max_body_size 0` and `proxy_request_buffering off` are important for large WebDAV uploads
  and chunked uploads.
- Notify Push uses WebSocket at `/push/ws` by default. If `[notify_push].path` is changed, update the
  exact WebSocket location to match, for example `location = /custom/ws`.
- The service also exposes WebDAV through `/remote.php/dav`, `/remote.php/webdav`, the standard
  `/remote.php/dav/files/{owner}` mount, and the root fallback. Keep all paths routed to the same
  upstream; do not strip prefixes in Nginx.
- `/metrics` is proxied by this site and still requires Basic Auth from the service.
- After changing the file, run `nginx -t` before reloading Nginx.
