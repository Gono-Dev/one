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
