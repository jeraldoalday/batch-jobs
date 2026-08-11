-- 0001_create_widgets: fixture migration used by migrate_test.go.

CREATE TABLE IF NOT EXISTS widgets (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);
