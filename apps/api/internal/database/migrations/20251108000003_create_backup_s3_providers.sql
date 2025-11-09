-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS backup_s3_providers (
    id TEXT PRIMARY KEY,
    backup_id TEXT NOT NULL REFERENCES backups(id) ON DELETE CASCADE,
    s3_provider_id TEXT NOT NULL REFERENCES s3_providers(id) ON DELETE CASCADE,
    s3_object_key TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(backup_id, s3_provider_id)
);

CREATE INDEX idx_backup_s3_providers_backup_id ON backup_s3_providers(backup_id);
CREATE INDEX idx_backup_s3_providers_provider_id ON backup_s3_providers(s3_provider_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_backup_s3_providers_provider_id;
DROP INDEX IF EXISTS idx_backup_s3_providers_backup_id;
DROP TABLE IF EXISTS backup_s3_providers;
-- +goose StatementEnd

