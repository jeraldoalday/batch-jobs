-- 0002_add_widget_index: fixture migration used by migrate_test.go.

CREATE INDEX IF NOT EXISTS idx_widgets_name ON widgets (name);
