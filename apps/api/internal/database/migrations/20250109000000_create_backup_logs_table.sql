-- +goose Up
-- +goose StatementBegin
-- Create a separate table for backup logs for better performance and scalability
-- This prevents logs from being cleared when updating backup status
-- Each log line is stored as a separate row for efficient querying
CREATE TABLE IF NOT EXISTS backup_logs (
    id TEXT PRIMARY KEY,
    backup_id TEXT NOT NULL REFERENCES backups(id) ON DELETE CASCADE,
    log_line TEXT NOT NULL,
    line_number INTEGER NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_backup_logs_backup_id ON backup_logs(backup_id);
CREATE INDEX IF NOT EXISTS idx_backup_logs_backup_id_line_number ON backup_logs(backup_id, line_number);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_backup_logs_backup_id_line_number;
DROP INDEX IF EXISTS idx_backup_logs_backup_id;
DROP TABLE IF EXISTS backup_logs;
-- +goose StatementEnd

