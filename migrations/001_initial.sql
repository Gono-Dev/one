CREATE TABLE IF NOT EXISTS users (
    username     TEXT PRIMARY KEY,
    display_name TEXT,
    enabled      INTEGER NOT NULL DEFAULT 1,
    created_at   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS app_passwords (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      TEXT    NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    label         TEXT    NOT NULL,
    password_hash TEXT    NOT NULL,
    created_at    INTEGER NOT NULL,
    last_used_at  INTEGER,
    expires_at    INTEGER,
    UNIQUE(username, label)
);

CREATE TABLE IF NOT EXISTS app_password_scopes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    app_password_id INTEGER NOT NULL REFERENCES app_passwords(id) ON DELETE CASCADE,
    mount_path      TEXT    NOT NULL,
    storage_path    TEXT    NOT NULL,
    permission      TEXT    NOT NULL CHECK(permission IN ('view', 'full')),
    created_at      INTEGER NOT NULL,
    UNIQUE(app_password_id, mount_path)
);

CREATE TABLE IF NOT EXISTS file_ids (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    owner       TEXT    NOT NULL,
    rel_path    TEXT    NOT NULL,
    etag        TEXT,
    permissions INTEGER,
    favorite    INTEGER NOT NULL DEFAULT 0,
    mtime_ns    INTEGER,
    file_size   INTEGER,
    created_at  INTEGER NOT NULL,
    UNIQUE(owner, rel_path)
);

CREATE TABLE IF NOT EXISTS change_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    owner       TEXT    NOT NULL,
    file_id     INTEGER NOT NULL,
    rel_path    TEXT    NOT NULL,
    operation   TEXT    NOT NULL CHECK(operation IN ('create','modify','delete','move')),
    sync_token  INTEGER NOT NULL,
    changed_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_changelog_owner_token
    ON change_log(owner, sync_token);

CREATE INDEX IF NOT EXISTS idx_changelog_owner_changed_at
    ON change_log(owner, changed_at);

CREATE TABLE IF NOT EXISTS sync_tokens (
    owner TEXT PRIMARY KEY,
    token INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS shares (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    token       TEXT    NOT NULL UNIQUE,
    owner       TEXT    NOT NULL,
    rel_path    TEXT    NOT NULL,
    permissions INTEGER NOT NULL DEFAULT 1,
    password    TEXT,
    expires_at  INTEGER,
    created_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS upload_sessions (
    upload_id   TEXT    PRIMARY KEY,
    owner       TEXT    NOT NULL,
    target_path TEXT    NOT NULL,
    total_size  INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_props (
    owner      TEXT    NOT NULL,
    rel_path   TEXT    NOT NULL,
    namespace  TEXT    NOT NULL,
    name       TEXT    NOT NULL,
    xml        BLOB    NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(owner, rel_path, namespace, name)
);

CREATE INDEX IF NOT EXISTS idx_dead_props_owner_path
    ON dead_props(owner, rel_path);

CREATE TABLE IF NOT EXISTS webdav_locks (
    token        TEXT PRIMARY KEY,
    path         TEXT    NOT NULL,
    principal    TEXT,
    owner_xml    TEXT,
    timeout_at   INTEGER,
    timeout_secs INTEGER,
    shared       INTEGER NOT NULL,
    deep         INTEGER NOT NULL,
    created_at   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_webdav_locks_path
    ON webdav_locks(path);

CREATE INDEX IF NOT EXISTS idx_webdav_locks_timeout
    ON webdav_locks(timeout_at);

CREATE TABLE IF NOT EXISTS settings (
    key        TEXT PRIMARY KEY,
    value_json TEXT    NOT NULL,
    updated_at INTEGER NOT NULL,
    updated_by TEXT
);
