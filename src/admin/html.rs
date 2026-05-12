use crate::{
    config::Config,
    db::{LocalAppPassword, LocalUser, BOOTSTRAP_USER},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeKind {
    Success,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub kind: NoticeKind,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OneTimePassword {
    pub username: String,
    pub label: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusPageData {
    pub sections: Vec<StatusSection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientsPageData {
    pub webdav_clients: Vec<StatusRow>,
    pub notify_connections: Vec<StatusRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusSection {
    pub title: String,
    pub rows: Vec<StatusRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusRow {
    pub label: String,
    pub value: String,
}

pub fn render_users_page(
    principal_username: &str,
    csrf_token: &str,
    config: &Config,
    users: &[LocalUser],
    notice: Option<&Notice>,
    one_time_password: Option<&OneTimePassword>,
) -> String {
    let mut rows = String::new();
    for user in users {
        rows.push_str(&render_user_row(user, csrf_token));
    }

    let notice_html = notice.map(render_notice).unwrap_or_default();
    let security_notice_html = render_http_security_notice(&config.server.base_url);
    let secret_html = one_time_password.map(render_secret).unwrap_or_default();
    let user_count = users.len();

    format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Gono Cloud Admin</title>
    <style>{STYLE}</style>
  </head>
  <body>
    <div class="admin-shell">
      <aside class="sidebar" aria-label="Admin navigation">
        <div class="brand">
          <span class="brand-mark">G</span>
          <div>
            <strong>Gono Cloud</strong>
            <span>Admin console</span>
          </div>
        </div>
        <nav class="nav-list">
          <a class="nav-item" href="/admin/users" aria-current="page">Users</a>
          <a class="nav-item" href="/admin/status">Status</a>
          <a class="nav-item" href="/admin/clients">Clients</a>
          <a class="nav-item" href="/admin/settings">Settings</a>
        </nav>
        <p class="sidebar-note">Signed in with Basic Auth. To switch users, clear browser credentials for this site.</p>
      </aside>
      <main class="admin-main">
        <div class="page-header">
          <h1>User Management</h1>
          <span>Admin: {principal}</span>
        </div>
        {secret_html}
        {security_notice_html}
        {notice_html}
        <section class="card" aria-labelledby="create-user-title">
          <div class="card-header">
            <h2 id="create-user-title">Create User</h2>
          </div>
          <form method="post" action="/admin/users" class="form-grid">
            <input type="hidden" name="_csrf" value="{csrf}">
            <div class="field">
              <label for="username">Username</label>
              <input class="input" id="username" name="username" placeholder="alice" required>
            </div>
            <div class="field">
              <label for="display_name">Display name</label>
              <input class="input" id="display_name" name="display_name" placeholder="Alice Example">
            </div>
            <button class="button button-primary" type="submit">Create</button>
          </form>
          <p class="help-text">Usernames may contain ASCII letters, numbers, dot, underscore, dash, or @.</p>
        </section>
        {create_password_html}
        <section class="card" aria-labelledby="users-title">
          <div class="card-header">
            <h2 id="users-title">Local Users ({user_count})</h2>
          </div>
          <div class="user-list">{rows}</div>
        </section>
      </main>
    </div>
    <script>{EXPIRY_SCRIPT}</script>
  </body>
</html>"#,
        principal = escape_html(principal_username),
        csrf = escape_attr(csrf_token),
        security_notice_html = security_notice_html,
        create_password_html = render_create_app_password_card(users, csrf_token),
    )
}

fn render_create_app_password_card(users: &[LocalUser], csrf_token: &str) -> String {
    if users.is_empty() {
        return String::new();
    }
    let default_expiration = default_expiration_value();
    let options = users
        .iter()
        .map(|user| {
            format!(
                r#"<option value="{username}">{label}</option>"#,
                username = escape_attr(&user.username),
                label = escape_html(&user.username)
            )
        })
        .collect::<String>();

    format!(
        r#"<section class="card" aria-labelledby="create-password-title">
  <div class="card-header">
    <h2 id="create-password-title">Create App Password</h2>
  </div>
  <form method="post" action="/admin/app-passwords" class="scope-form-grid">
    <input type="hidden" name="_csrf" value="{csrf}">
    <div class="field">
      <label for="password-username">Username</label>
      <select class="input" id="password-username" name="username">{options}</select>
    </div>
    <div class="field">
      <label for="password-label">Label</label>
      <input class="input" id="password-label" name="label" placeholder="mobile-sync" required>
    </div>
    <div class="field">
      <label for="password-mount">WebDAV start point</label>
      <input class="input" id="password-mount" name="mount_path" value="/" required>
    </div>
    <div class="field">
      <label for="password-storage">Storage path</label>
      <input class="input" id="password-storage" name="storage_path" value="/" required>
    </div>
    <div class="field">
      <label for="password-permission">Permission</label>
      <select class="input" id="password-permission" name="permission">
        <option value="full">Full access</option>
        <option value="view">view</option>
      </select>
    </div>
    <div class="field">
      <label for="password-expiry">Expires</label>
      <button class="button button-secondary" id="password-expiry" type="button" popovertarget="create-password-expiry">Set expiry</button>
      <div class="modal-popover" id="create-password-expiry" popover>
        <h4>Edit app password expiry</h4>
        <p>Set when the new app password expires, or keep it valid forever.</p>
        <div class="modal-fields">
          <div class="field">
            <label for="create-password-expiry-mode">Expiry</label>
            <select class="input" id="create-password-expiry-mode" name="expires_at_mode">
              <option value="never">Never</option>
              <option value="at">At time</option>
            </select>
          </div>
          <div class="field" data-expiry-time-field hidden>
            <label for="create-password-expiry-input">Expiration time</label>
            <input class="input" id="create-password-expiry-input" name="expires_at" type="text" value="{default_expiration}" placeholder="YYYY-MM-DDTHH:MM" autocomplete="off">
          </div>
        </div>
        <div class="modal-actions">
          <button class="button" type="button" popovertarget="create-password-expiry" popovertargetaction="hide">Cancel</button>
          <button class="button button-primary" type="button" popovertarget="create-password-expiry" popovertargetaction="hide">Save</button>
        </div>
      </div>
    </div>
    <button class="button button-primary" type="submit">Create</button>
  </form>
</section>"#,
        csrf = escape_attr(csrf_token),
        default_expiration = escape_attr(&default_expiration),
    )
}

pub fn render_settings_page(principal_username: &str, config: &Config) -> String {
    let security_notice_html = render_http_security_notice(&config.server.base_url);
    let advertised_types = config.notify_push.advertised_types.join("\n");
    let admin_users = config.admin.users.join("\n");
    let server_base_url = readonly_input_text_with_note(
        "server_base_url",
        "Base URL",
        &config.server.base_url,
        if config.server.base_url.trim().is_empty() {
            "Empty value; runtime responses infer the origin from each request."
        } else {
            "runtime inference is not used for generated endpoints."
        },
    );
    let server_inferred_base_url = readonly_code_with_note(
        "Runtime inferred Base URL",
        "runtime-base-url",
        "Detecting...",
    );
    let server_bind = readonly_text("Bind", &config.server.bind);
    let server_cert_file = readonly_text("TLS cert file", &config.server.cert_file);
    let server_key_file = readonly_text("TLS key file", &config.server.key_file);
    let auth_realm = readonly_input_text("auth_realm", "Realm", &config.auth.realm);
    let sync_retention = readonly_input_number(
        "sync_change_log_retention_days",
        "Change log retention days",
        config.sync.change_log_retention_days,
    );
    let sync_min_entries = readonly_input_number(
        "sync_change_log_min_entries",
        "Change log min entries",
        config.sync.change_log_min_entries,
    );
    let notify_enabled =
        readonly_input_bool("notify_push_enabled", "Enabled", config.notify_push.enabled);
    let notify_path = readonly_input_text("notify_push_path", "Path", &config.notify_push.path);
    let notify_pre_auth = readonly_input_number(
        "notify_push_pre_auth_ttl_secs",
        "Pre-auth TTL seconds",
        config.notify_push.pre_auth_ttl_secs,
    );
    let notify_limit = readonly_input_number(
        "notify_push_user_connection_limit",
        "User connection limit",
        config.notify_push.user_connection_limit,
    );
    let notify_debounce = readonly_input_number(
        "notify_push_max_debounce_secs",
        "Max debounce seconds",
        config.notify_push.max_debounce_secs,
    );
    let notify_ping = readonly_input_number(
        "notify_push_ping_interval_secs",
        "Ping interval seconds",
        config.notify_push.ping_interval_secs,
    );
    let notify_auth_timeout = readonly_input_number(
        "notify_push_auth_timeout_secs",
        "Auth timeout seconds",
        config.notify_push.auth_timeout_secs,
    );
    let notify_max_connection = readonly_input_number(
        "notify_push_max_connection_secs",
        "Max connection seconds",
        config.notify_push.max_connection_secs,
    );
    let admin_enabled = readonly_input_bool("admin_enabled", "Enabled", config.admin.enabled);
    let storage_data_dir = readonly_text("Data dir", &config.storage.data_dir);
    let storage_xattr_ns = readonly_text("Xattr namespace", &config.storage.xattr_ns);
    let storage_upload_min_free_bytes = readonly_text(
        "Upload minimum free bytes",
        config.storage.upload_min_free_bytes,
    );
    let storage_upload_min_free_percent = readonly_text(
        "Upload minimum free percent",
        format!("{}%", config.storage.upload_min_free_percent.min(100)),
    );
    let db_path = readonly_text("Path", &config.db.path);
    let db_max_connections = readonly_text("Max connections", config.db.max_connections);

    format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Gono Cloud Settings</title>
    <style>{STYLE}</style>
  </head>
  <body>
    <div class="admin-shell">
      <aside class="sidebar" aria-label="Admin navigation">
        <div class="brand">
          <span class="brand-mark">G</span>
          <div>
            <strong>Gono Cloud</strong>
            <span>Admin console</span>
          </div>
        </div>
        <nav class="nav-list">
          <a class="nav-item" href="/admin/users">Users</a>
          <a class="nav-item" href="/admin/status">Status</a>
          <a class="nav-item" href="/admin/clients">Clients</a>
          <a class="nav-item" href="/admin/settings" aria-current="page">Settings</a>
        </nav>
        <p class="sidebar-note">Runtime configuration is read from config.toml when the service starts.</p>
      </aside>
      <main class="admin-main">
        <div class="page-header">
          <h1>Settings</h1>
          <span>Admin: {principal}</span>
        </div>
        {security_notice_html}
        <div class="notice notice-info" role="status">Edit config.toml and restart gono-cloud to change these values.</div>
        <div class="settings-form">
          <section class="card" aria-labelledby="server-settings-title">
            <div class="card-header"><h2 id="server-settings-title">Server</h2></div>
            <div class="settings-grid">
              {server_base_url}
              {server_inferred_base_url}
              {server_bind}
              {server_cert_file}
              {server_key_file}
            </div>
          </section>
          <section class="card" aria-labelledby="auth-settings-title">
            <div class="card-header"><h2 id="auth-settings-title">Auth</h2></div>
            <div class="settings-grid">
              {auth_realm}
            </div>
          </section>
          <section class="card" aria-labelledby="sync-settings-title">
            <div class="card-header"><h2 id="sync-settings-title">Sync</h2></div>
            <div class="settings-grid">
              {sync_retention}
              {sync_min_entries}
            </div>
          </section>
          <section class="card" aria-labelledby="notify-settings-title">
            <div class="card-header"><h2 id="notify-settings-title">Notify Push</h2></div>
            <div class="settings-grid">
              {notify_enabled}
              {notify_path}
              {notify_pre_auth}
              {notify_limit}
              {notify_debounce}
              {notify_ping}
              {notify_auth_timeout}
              {notify_max_connection}
              <div class="field field-wide">
                <label for="notify_push_advertised_types">Advertised types</label>
                <textarea class="input textarea" id="notify_push_advertised_types" name="notify_push_advertised_types" rows="3" readonly aria-readonly="true">{advertised_types}</textarea>
              </div>
            </div>
          </section>
          <section class="card" aria-labelledby="admin-settings-title">
            <div class="card-header"><h2 id="admin-settings-title">Admin</h2></div>
            <div class="settings-grid">
              {admin_enabled}
              <div class="field field-wide">
                <label for="admin_users">Admin users</label>
                <textarea class="input textarea" id="admin_users" name="admin_users" rows="3" readonly aria-readonly="true">{admin_users}</textarea>
              </div>
            </div>
          </section>
          <section class="card" aria-labelledby="storage-settings-title">
            <div class="card-header"><h2 id="storage-settings-title">Storage</h2></div>
            <div class="settings-grid">
              {storage_data_dir}
              {storage_xattr_ns}
              {storage_upload_min_free_bytes}
              {storage_upload_min_free_percent}
            </div>
          </section>
          <section class="card" aria-labelledby="db-settings-title">
            <div class="card-header"><h2 id="db-settings-title">Database</h2></div>
            <div class="settings-grid">
              {db_path}
              {db_max_connections}
            </div>
          </section>
        </div>
      </main>
    </div>
    <script>
      (() => {{
        const value = document.getElementById("runtime-base-url");
        const note = document.getElementById("runtime-base-url-note");
        if (!value || !note) return;
        const origin = window.location.origin || `${{window.location.protocol}}//${{window.location.host}}`;
        value.textContent = origin;
        note.textContent = "Inferred in this browser from the current Admin request.";
      }})();
    </script>
  </body>
</html>"#,
        principal = escape_html(principal_username),
        security_notice_html = security_notice_html,
        advertised_types = escape_html(&advertised_types),
        admin_users = escape_html(&admin_users),
    )
}

pub fn render_status_page(
    principal_username: &str,
    config: &Config,
    status: &StatusPageData,
) -> String {
    let security_notice_html = render_http_security_notice(&config.server.base_url);
    let sections = render_status_sections(&status.sections);

    format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Gono Cloud Status</title>
    <style>{STYLE}</style>
  </head>
  <body>
    <div class="admin-shell">
      <aside class="sidebar" aria-label="Admin navigation">
        <div class="brand">
          <span class="brand-mark">G</span>
          <div>
            <strong>Gono Cloud</strong>
            <span>Admin console</span>
          </div>
        </div>
        <nav class="nav-list">
          <a class="nav-item" href="/admin/users">Users</a>
          <a class="nav-item" href="/admin/status" aria-current="page">Status</a>
          <a class="nav-item" href="/admin/clients">Clients</a>
          <a class="nav-item" href="/admin/settings">Settings</a>
        </nav>
        <p class="sidebar-note">Runtime counters are sampled from the current process.</p>
      </aside>
      <main class="admin-main">
        <div class="page-header">
          <h1>System Status</h1>
          <span>Admin: {principal}</span>
        </div>
        {security_notice_html}
        {sections}
      </main>
    </div>
  </body>
</html>"#,
        principal = escape_html(principal_username),
        security_notice_html = security_notice_html,
        sections = sections,
    )
}

pub fn render_clients_page(
    principal_username: &str,
    config: &Config,
    clients: &ClientsPageData,
) -> String {
    let security_notice_html = render_http_security_notice(&config.server.base_url);
    let webdav_clients = render_status_client_section(
        "status-webdav-clients",
        "WebDAV Clients",
        "No recent WebDAV clients.",
        &clients.webdav_clients,
    );
    let notify_connections = render_status_client_section(
        "status-notify-clients",
        "Notify Push Clients",
        "No active notify_push clients.",
        &clients.notify_connections,
    );

    format!(
        r#"<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Gono Cloud Clients</title>
    <style>{STYLE}</style>
  </head>
  <body>
    <div class="admin-shell">
      <aside class="sidebar" aria-label="Admin navigation">
        <div class="brand">
          <span class="brand-mark">G</span>
          <div>
            <strong>Gono Cloud</strong>
            <span>Admin console</span>
          </div>
        </div>
        <nav class="nav-list">
          <a class="nav-item" href="/admin/users">Users</a>
          <a class="nav-item" href="/admin/status">Status</a>
          <a class="nav-item" href="/admin/clients" aria-current="page">Clients</a>
          <a class="nav-item" href="/admin/settings">Settings</a>
        </nav>
        <p class="sidebar-note">Client activity is sampled from the current process.</p>
      </aside>
      <main class="admin-main">
        <div class="page-header">
          <h1>Clients</h1>
          <span>Admin: {principal}</span>
        </div>
        {security_notice_html}
        {webdav_clients}
        {notify_connections}
      </main>
    </div>
  </body>
</html>"#,
        principal = escape_html(principal_username),
        security_notice_html = security_notice_html,
        webdav_clients = webdav_clients,
        notify_connections = notify_connections,
    )
}

fn render_status_sections(sections: &[StatusSection]) -> String {
    sections
        .iter()
        .map(|section| {
            format!(
                r#"<section class="card" aria-labelledby="{id}">
  <div class="card-header"><h2 id="{id}">{title}</h2></div>
  <div class="status-table">{rows}</div>
</section>"#,
                id = escape_attr(&format!("status-{}", status_anchor(&section.title))),
                title = escape_html(&section.title),
                rows = render_status_rows(&section.rows),
            )
        })
        .collect::<String>()
}

fn render_status_client_section(
    id: &str,
    title: &str,
    empty_text: &str,
    rows: &[StatusRow],
) -> String {
    let body = if rows.is_empty() {
        format!(r#"<p class="status-empty">{}</p>"#, escape_html(empty_text))
    } else {
        format!(
            r#"<div class="status-table">{}</div>"#,
            render_status_rows(rows)
        )
    };
    format!(
        r#"<section class="card" aria-labelledby="{id}">
  <div class="card-header"><h2 id="{id}">{title}</h2></div>
  {body}
</section>"#,
        id = escape_attr(id),
        title = escape_html(title),
        body = body,
    )
}

fn render_status_rows(rows: &[StatusRow]) -> String {
    rows.iter()
        .map(|row| {
            format!(
                r#"<div class="status-row"><span>{label}</span><strong>{value}</strong></div>"#,
                label = escape_html(&row.label),
                value = escape_html(&row.value),
            )
        })
        .collect::<String>()
}

fn status_anchor(title: &str) -> String {
    title
        .chars()
        .filter_map(|ch| {
            if ch.is_ascii_alphanumeric() {
                Some(ch.to_ascii_lowercase())
            } else if ch.is_whitespace() {
                Some('-')
            } else {
                None
            }
        })
        .collect()
}

fn render_http_security_notice(base_url: &str) -> String {
    if !base_url.trim_start().starts_with("http://") {
        return String::new();
    }

    format!(
        r#"<div class="notice notice-warning" role="status">Admin is configured with an HTTP base URL: <strong>{base_url}</strong>. Basic Auth credentials and app passwords can be exposed over plain HTTP; use HTTPS through Nginx or another reverse proxy before accessing this page from other machines.</div>"#,
        base_url = escape_html(base_url)
    )
}

fn readonly_input_text(name: &str, label: &str, value: impl ToString) -> String {
    let name_attr = escape_attr(name);
    format!(
        r#"<div class="field">
  <label for="{name}">{label}</label>
  <input class="input" id="{name}" name="{name}" value="{value}" readonly aria-readonly="true">
</div>"#,
        name = name_attr,
        label = escape_html(label),
        value = escape_attr(&value.to_string())
    )
}

fn readonly_input_text_with_note(
    name: &str,
    label: &str,
    value: impl ToString,
    note: &str,
) -> String {
    let name_attr = escape_attr(name);
    format!(
        r#"<div class="field">
  <label for="{name}">{label}</label>
  <input class="input" id="{name}" name="{name}" value="{value}" readonly aria-readonly="true">
  <span class="field-note">{note}</span>
</div>"#,
        name = name_attr,
        label = escape_html(label),
        value = escape_attr(&value.to_string()),
        note = escape_html(note)
    )
}

fn readonly_input_number(name: &str, label: &str, value: impl ToString) -> String {
    let name_attr = escape_attr(name);
    format!(
        r#"<div class="field">
  <label for="{name}">{label}</label>
  <input class="input" id="{name}" name="{name}" type="number" min="0" value="{value}" readonly aria-readonly="true">
</div>"#,
        name = name_attr,
        label = escape_html(label),
        value = escape_attr(&value.to_string())
    )
}

fn readonly_input_bool(name: &str, label: &str, value: bool) -> String {
    let true_selected = if value { " selected" } else { "" };
    let false_selected = if value { "" } else { " selected" };
    let name_attr = escape_attr(name);
    format!(
        r#"<div class="field">
  <label for="{name}">{label}</label>
  <select class="input" id="{name}" name="{name}" disabled aria-disabled="true">
    <option value="true"{true_selected}>Enabled</option>
    <option value="false"{false_selected}>Disabled</option>
  </select>
</div>"#,
        name = name_attr,
        label = escape_html(label),
    )
}

fn readonly_text(label: &str, value: impl ToString) -> String {
    format!(
        r#"<div class="readonly-field">
  <span>{label}</span>
  <code>{value}</code>
</div>"#,
        label = escape_html(label),
        value = escape_html(&value.to_string())
    )
}

fn readonly_code_with_note(label: &str, id: &str, value: impl ToString) -> String {
    format!(
        r#"<div class="readonly-field">
  <span>{label}</span>
  <code id="{id}">{value}</code>
  <small id="{id}-note" class="field-note"></small>
</div>"#,
        label = escape_html(label),
        id = escape_attr(id),
        value = escape_html(&value.to_string())
    )
}

fn render_notice(notice: &Notice) -> String {
    let class_name = match notice.kind {
        NoticeKind::Success => "notice notice-success",
        NoticeKind::Error => "notice notice-error",
    };
    format!(
        r#"<div class="{class_name}" role="status">{}</div>"#,
        escape_html(&notice.text)
    )
}

fn render_secret(secret: &OneTimePassword) -> String {
    format!(
        r#"<section class="secret-panel" aria-label="App password created">
  <strong>App password created for {username} ({label})</strong>
  <code>{password}</code>
  <p>Show this value only once after creating or resetting a user.</p>
</section>"#,
        username = escape_html(&secret.username),
        label = escape_html(&secret.label),
        password = escape_html(&secret.password)
    )
}

fn render_user_row(user: &LocalUser, csrf_token: &str) -> String {
    let username = escape_html(&user.username);
    let username_attr = escape_attr(&user.username);
    let display_name = escape_html(&user.display_name);
    let labels = if user.app_password_labels.is_empty() {
        "none".to_owned()
    } else {
        escape_html(&user.app_password_labels.join(", "))
    };
    let app_passwords = user
        .app_passwords
        .iter()
        .map(|password| render_app_password_block(user, password, csrf_token))
        .collect::<String>();
    let status = if user.enabled { "enabled" } else { "disabled" };
    let status_class = if user.enabled {
        "badge"
    } else {
        "badge badge-danger"
    };
    let toggle = if user.enabled {
        render_post_button(
            &format!("/admin/users/{}/disable", user.username),
            csrf_token,
            "Disable",
            "button button-secondary button-mini",
        )
    } else {
        render_post_button(
            &format!("/admin/users/{}/enable", user.username),
            csrf_token,
            "Enable",
            "button button-secondary button-mini",
        )
    };
    let delete = if user.username == BOOTSTRAP_USER {
        r#"<button class="button button-danger button-mini" type="button" disabled>Delete</button>"#
            .to_owned()
    } else {
        render_post_button(
            &format!("/admin/users/{}/delete", user.username),
            csrf_token,
            "Delete",
            "button button-danger button-mini",
        )
    };

    format!(
        r#"<article class="user-row">
  <div class="user-summary-line">
    <div class="user-name-cluster">
      <h3 class="user-name">{username}</h3>
      <span class="{status_class}">{status}</span>
      <span class="display-meta">Display name: <strong>{display_name}</strong></span>
    </div>
    <div class="user-toolbar">
      {toggle}
      <button class="button button-secondary button-mini" type="button" popovertarget="display-name-{username_attr}">Display name</button>
      {delete}
    </div>
  </div>
  <div class="modal-popover" id="display-name-{username_attr}" popover>
    <form method="post" action="/admin/users/{username_attr}/display-name">
      <input type="hidden" name="_csrf" value="{csrf}">
      <h4>Edit display name</h4>
      <p>Set the display name for {username}.</p>
      <div class="field">
        <label for="display-name-{username_attr}-input">Display name</label>
        <input class="input" id="display-name-{username_attr}-input" name="display_name" value="{display_name_attr}" autocomplete="off" required>
      </div>
      <div class="modal-actions">
        <button class="button" type="button" popovertarget="display-name-{username_attr}" popovertargetaction="hide">Cancel</button>
        <button class="button button-primary" type="submit">Save</button>
      </div>
    </form>
  </div>
  <p class="meta-line">Created {created_at} - app passwords: {password_count} ({labels})</p>
  <div class="password-list">{app_passwords}</div>
</article>"#,
        csrf = escape_attr(csrf_token),
        display_name_attr = escape_attr(&user.display_name),
        created_at = escape_html(&format_timestamp(user.created_at)),
        password_count = user.app_password_count,
        app_passwords = app_passwords,
    )
}

fn render_app_password_block(
    user: &LocalUser,
    password: &LocalAppPassword,
    csrf_token: &str,
) -> String {
    let username_attr = escape_attr(&user.username);
    let label_attr = escape_attr(&password.label);
    let scope_rows = password
        .scopes
        .iter()
        .map(|scope| {
            format!(
                r#"<tr><td>{mount}</td><td>{storage}</td><td>{permission}</td></tr>"#,
                mount = escape_html(&scope.mount_path),
                storage = escape_html(&scope.storage_path),
                permission = escape_html(scope.permission.display_label())
            )
        })
        .collect::<String>();
    let expires_text = password
        .expires_at
        .map(format_timestamp)
        .unwrap_or_else(|| "Never".to_owned());
    let expires_value = password
        .expires_at
        .and_then(|value| chrono::DateTime::from_timestamp(value, 0))
        .map(|value| value.format("%Y-%m-%dT%H:%M").to_string())
        .unwrap_or_else(default_expiration_value);
    let expires_time_hidden = if password.expires_at.is_none() {
        " hidden"
    } else {
        ""
    };
    let delete = if user.app_password_count <= 1 {
        r#"<button class="button button-danger button-mini" type="button" disabled>Delete</button>"#
            .to_owned()
    } else {
        render_post_button(
            &format!(
                "/admin/users/{}/app-passwords/{}/delete",
                user.username, password.label
            ),
            csrf_token,
            "Delete",
            "button button-danger button-mini",
        )
    };
    let expiry_popover_id = format!("expiry-{username_attr}-{label_attr}");
    let expiry_form = format!(
        r#"<button class="button button-secondary button-mini" type="button" popovertarget="{expiry_popover_id}">Set expiry</button>
  <div class="modal-popover" id="{expiry_popover_id}" popover>
    <form method="post" action="/admin/users/{username_attr}/app-passwords/{label_attr}/expires-at">
      <input type="hidden" name="_csrf" value="{csrf}">
      <h4>Edit app password expiry</h4>
      <p>Set when {label} expires, or keep it valid forever.</p>
      <div class="modal-fields">
        <div class="field">
          <label for="{expiry_popover_id}-mode">Expiry</label>
          <select class="input" id="{expiry_popover_id}-mode" name="expires_at_mode">
            <option value="never"{never_selected}>Never</option>
            <option value="at"{at_selected}>At time</option>
          </select>
        </div>
        <div class="field" data-expiry-time-field{expires_time_hidden}>
          <label for="{expiry_popover_id}-input">Expiration time</label>
          <input class="input" id="{expiry_popover_id}-input" name="expires_at" type="text" value="{expires_value}" placeholder="YYYY-MM-DDTHH:MM" autocomplete="off">
        </div>
      </div>
      <div class="modal-actions">
        <button class="button" type="button" popovertarget="{expiry_popover_id}" popovertargetaction="hide">Cancel</button>
        <button class="button button-primary" type="submit">Save</button>
      </div>
    </form>
  </div>"#,
        username_attr = username_attr,
        label_attr = label_attr,
        label = escape_html(&password.label),
        expiry_popover_id = expiry_popover_id,
        csrf = escape_attr(csrf_token),
        expires_value = escape_attr(&expires_value),
        expires_time_hidden = expires_time_hidden,
        never_selected = if password.expires_at.is_none() {
            " selected"
        } else {
            ""
        },
        at_selected = if password.expires_at.is_some() {
            " selected"
        } else {
            ""
        },
    );

    format!(
        r#"<section class="password-block" aria-label="{label} app password">
  <div class="password-header">
    <div>
      <strong>{label}</strong>
      <span class="meta-line">Created {created_at} - Last used {last_used_at} - Expires {expires_text}</span>
    </div>
    <div class="user-toolbar">
      {expiry_form}
      {reset}
      {delete}
    </div>
  </div>
  <table class="scope-table">
    <thead><tr><th>WebDAV start point</th><th>Storage path</th><th>Permission</th></tr></thead>
    <tbody>{scope_rows}</tbody>
  </table>
</section>"#,
        label = escape_html(&password.label),
        created_at = escape_html(&format_timestamp(password.created_at)),
        last_used_at = password
            .last_used_at
            .map(format_timestamp)
            .unwrap_or_else(|| "Never".to_owned()),
        expires_text = escape_html(&expires_text),
        expiry_form = expiry_form,
        reset = render_post_button(
            &format!(
                "/admin/users/{}/app-passwords/{}/reset-password",
                user.username, password.label
            ),
            csrf_token,
            "Reset",
            "button button-secondary button-mini",
        ),
        delete = delete,
        scope_rows = scope_rows,
    )
}

fn render_post_button(action: &str, csrf_token: &str, label: &str, class_name: &str) -> String {
    format!(
        r#"<form method="post" action="{action}">
  <input type="hidden" name="_csrf" value="{csrf}">
  <button class="{class_name}" type="submit">{label}</button>
</form>"#,
        action = escape_attr(action),
        csrf = escape_attr(csrf_token),
        class_name = escape_attr(class_name),
        label = escape_html(label)
    )
}

fn format_timestamp(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|value| value.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| timestamp.to_string())
}

fn default_expiration_value() -> String {
    (chrono::Utc::now() + chrono::Duration::days(30))
        .format("%Y-%m-%dT%H:%M")
        .to_string()
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn escape_attr(value: &str) -> String {
    escape_html(value)
}

const EXPIRY_SCRIPT: &str = r#"
document.querySelectorAll('select[name="expires_at_mode"]').forEach((select) => {
  const syncExpiryField = () => {
    const fields = select.closest('.modal-fields');
    const timeField = fields && fields.querySelector('[data-expiry-time-field]');
    if (timeField) {
      timeField.hidden = select.value !== 'at';
    }
  };
  select.addEventListener('change', syncExpiryField);
  syncExpiryField();
});
"#;

const STYLE: &str = r#"
:root {
  color-scheme: light;
  --bg: #f5f7fb;
  --surface: #ffffff;
  --surface-muted: #f8fafc;
  --border: #dce3ec;
  --text: #172033;
  --muted: #61708a;
  --muted-strong: #344256;
  --primary: #071329;
  --danger: #e5484d;
  --success: #15803d;
  --success-bg: #dcfce7;
  --radius: 8px;
}
* { box-sizing: border-box; }
[hidden] { display: none !important; }
body {
  margin: 0;
  color: var(--text);
  background: var(--bg);
  font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  font-size: 14px;
}
.admin-shell {
  width: min(100%, 1320px);
  min-height: 100vh;
  margin: 0 auto;
  display: grid;
  grid-template-columns: 226px minmax(0, 1fr);
  background: var(--bg);
  border-left: 1px solid var(--border);
  border-right: 1px solid var(--border);
}
.sidebar {
  position: sticky;
  top: 0;
  height: 100vh;
  display: grid;
  grid-template-rows: auto 1fr auto;
  gap: 20px;
  padding: 24px 18px;
  background: var(--surface);
  border-right: 1px solid var(--border);
}
.brand { display: flex; align-items: center; gap: 10px; }
.brand-mark {
  width: 32px;
  height: 32px;
  display: grid;
  place-items: center;
  color: white;
  background: var(--primary);
  border-radius: 50%;
  font-weight: 800;
}
.brand strong, .brand span { display: block; }
.brand span, .sidebar-note, .help-text, .meta-line, .display-meta, .field-note {
  color: var(--muted);
  font-size: 12px;
  font-weight: 600;
}
.nav-list { display: grid; align-content: start; gap: 6px; }
.nav-item {
  padding: 9px 10px;
  color: var(--muted-strong);
  text-decoration: none;
  border-radius: 6px;
  font-weight: 800;
}
.nav-item[aria-current="page"] { background: #e8eef6; color: var(--text); }
.admin-main {
  width: min(100%, 1040px);
  margin: 0 auto;
  padding: 28px 24px 48px;
}
.page-header {
  display: flex;
  align-items: end;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 18px;
}
h1, h2, h3, h4, p { margin-top: 0; }
h1 { margin-bottom: 0; font-size: 26px; }
.card, .secret-panel, .notice {
  margin-bottom: 18px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
}
.card-header { padding: 18px 20px; border-bottom: 1px solid var(--border); }
.card-header h2 { margin: 0; font-size: 16px; }
.form-grid {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto;
  align-items: end;
  gap: 12px;
  padding: 18px 20px 10px;
}
.scope-form-grid {
  display: grid;
  grid-template-columns: repeat(6, minmax(0, 1fr)) auto;
  align-items: end;
  gap: 12px;
  padding: 18px 20px;
}
.settings-form { display: grid; gap: 0; }
.settings-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px;
  padding: 18px 20px;
}
.field-wide { grid-column: 1 / -1; }
.field { display: grid; gap: 7px; }
.field-note { line-height: 1.35; }
label { color: var(--muted-strong); font-size: 12px; font-weight: 800; }
.input {
  width: 100%;
  min-height: 38px;
  padding: 8px 10px;
  color: var(--text);
  background: white;
  border: 1px solid var(--border);
  border-radius: 6px;
  font: inherit;
  font-weight: 650;
}
.textarea {
  min-height: 88px;
  resize: vertical;
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
  font-size: 12px;
  line-height: 1.45;
}
.input[readonly],
.input:disabled,
.textarea[readonly] {
  color: var(--muted-strong);
  background: #edf3f8;
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
  font-size: 12px;
  font-weight: 400;
  opacity: 1;
  -webkit-text-fill-color: var(--muted-strong);
}
.compact-input { min-height: 32px; }
.button {
  min-height: 34px;
  padding: 7px 12px;
  color: var(--muted-strong);
  background: white;
  border: 1px solid var(--border);
  border-radius: 6px;
  cursor: pointer;
  font: inherit;
  font-weight: 800;
  text-decoration: none;
}
.button-primary { color: white; background: var(--primary); border-color: var(--primary); }
.button-secondary { background: var(--surface-muted); }
.button-danger { color: var(--danger); border-color: #fecaca; background: white; }
.button-mini { min-height: 28px; padding: 5px 9px; font-size: 12px; }
.button:disabled { cursor: not-allowed; opacity: 0.55; }
.help-text { padding: 0 20px 18px; }
.secret-panel, .notice { padding: 14px 18px; }
.secret-panel code {
  display: block;
  width: fit-content;
  max-width: 100%;
  margin: 10px 0 6px;
  padding: 8px 10px;
  overflow-wrap: anywhere;
  color: #052e1b;
  background: white;
  border: 1px solid #abefc6;
  border-radius: 6px;
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
}
.notice-success { color: #14532d; background: #f0fdf4; border-color: #bbf7d0; }
.notice-info { color: #164e63; background: #ecfeff; border-color: #a5f3fc; }
.notice-warning { color: #854d0e; background: #fffbeb; border-color: #fde68a; }
.notice-error { color: #991b1b; background: #fef2f2; border-color: #fecaca; }
.user-list { display: grid; }
.user-row { display: grid; gap: 10px; padding: 14px 20px; border-top: 1px solid var(--border); }
.user-row:first-child { border-top: 0; }
.user-summary-line {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 10px 16px;
}
.user-name-cluster, .user-toolbar, .row-actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
}
.user-toolbar, .row-actions { justify-content: flex-end; }
.user-toolbar form, .row-actions form { margin: 0; }
.user-name { margin: 0; font-size: 14px; overflow-wrap: anywhere; }
.badge {
  display: inline-flex;
  align-items: center;
  min-height: 20px;
  padding: 2px 7px;
  color: #315496;
  background: #e8f0ff;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 800;
}
.badge-danger { color: var(--danger); background: #fee2e2; }
.display-meta strong { margin-left: 3px; color: var(--muted-strong); }
.meta-line { margin-bottom: 0; }
.password-list { display: grid; gap: 10px; }
.password-block {
  display: grid;
  gap: 10px;
  padding: 12px;
  background: var(--surface-muted);
  border: 1px solid var(--border);
  border-radius: 6px;
}
.password-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 10px;
}
.password-header strong { display: block; margin-bottom: 4px; }
.scope-table {
  width: 100%;
  border-collapse: collapse;
  background: white;
  border: 1px solid var(--border);
  border-radius: 6px;
  overflow: hidden;
}
.scope-table th, .scope-table td {
  padding: 8px 10px;
  border-bottom: 1px solid var(--border);
  text-align: left;
  vertical-align: top;
  overflow-wrap: anywhere;
}
.scope-table th {
  color: var(--muted-strong);
  background: #edf3f8;
  font-size: 12px;
  font-weight: 800;
}
.scope-table tr:last-child td { border-bottom: 0; }
.readonly-field {
  display: grid;
  gap: 7px;
  min-width: 0;
}
.readonly-field span {
  color: var(--muted-strong);
  font-size: 12px;
  font-weight: 800;
}
.readonly-field code {
  display: block;
  min-height: 38px;
  padding: 9px 10px;
  overflow-wrap: anywhere;
  color: var(--muted-strong);
  background: #edf3f8;
  border: 1px solid var(--border);
  border-radius: 6px;
  font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}
.settings-actions {
  display: flex;
  justify-content: flex-end;
  margin-bottom: 18px;
}
.status-table { display: grid; }
.status-row {
  display: grid;
  grid-template-columns: minmax(150px, 30%) minmax(0, 1fr);
  gap: 12px;
  padding: 10px 20px;
  border-top: 1px solid var(--border);
}
.status-row:first-child { border-top: 0; }
.status-row span {
  color: var(--muted-strong);
  font-size: 12px;
  font-weight: 800;
}
.status-row strong {
  min-width: 0;
  overflow-wrap: anywhere;
  font-size: 13px;
}
.status-empty {
  margin: 0;
  padding: 14px 20px;
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}
.modal-popover {
  position: fixed;
  inset: 0;
  width: min(92vw, 360px);
  height: fit-content;
  max-height: min(90vh, 420px);
  overflow: auto;
  margin: auto;
  padding: 22px;
  color: var(--text);
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  box-shadow: 0 20px 60px rgba(15, 23, 42, 0.24);
}
.modal-popover::backdrop { background: rgba(15, 23, 42, 0.5); }
.modal-popover h4 { margin-bottom: 12px; font-size: 16px; }
.modal-popover p { color: var(--muted); font-size: 12px; font-weight: 600; }
.modal-popover form { margin: 0; }
.modal-fields { display: grid; gap: 12px; }
.modal-actions {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 10px;
  margin-top: 18px;
}
@media (max-width: 860px) {
  .admin-shell { width: 100%; grid-template-columns: 1fr; border: 0; }
  .sidebar { position: static; height: auto; border-right: 0; border-bottom: 1px solid var(--border); }
  .admin-main { padding: 18px 12px 32px; }
  .form-grid, .scope-form-grid, .settings-grid { grid-template-columns: 1fr; }
  .status-row { grid-template-columns: 1fr; }
  .field-wide { grid-column: auto; }
  .user-summary-line { align-items: flex-start; flex-direction: column; }
  .user-toolbar, .row-actions { justify-content: flex-start; }
}
"#;
