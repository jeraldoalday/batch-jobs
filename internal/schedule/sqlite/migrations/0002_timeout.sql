-- 0002_timeout: per-attempt execution timeout applied to every job a
-- schedule produces, mirroring queue/sqlite's migration 0002.

ALTER TABLE schedules ADD COLUMN timeout_ns INTEGER NOT NULL DEFAULT 0;
