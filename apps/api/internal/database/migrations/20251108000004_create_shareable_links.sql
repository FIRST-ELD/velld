-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS shareable_links (
    id TEXT PRIMARY KEY,
    backup_id TEXT NOT NULL REFERENCES backups(id) ON DELETE CASCADE,
    s3_provider_id TEXT REFERENCES s3_providers(id) ON DELETE SET NULL,
    token TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    access_count INTEGER DEFAULT 0
);

CREATE INDEX idx_shareable_links_token ON shareable_links(token);
CREATE INDEX idx_shareable_links_backup_id ON shareable_links(backup_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_shareable_links_backup_id;
DROP INDEX IF EXISTS idx_shareable_links_token;
DROP TABLE IF EXISTS shareable_links;
-- +goose StatementEnd

