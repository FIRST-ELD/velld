-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS s3_providers (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    region TEXT,
    bucket TEXT NOT NULL,
    access_key TEXT NOT NULL,
    secret_key TEXT NOT NULL,
    use_ssl INTEGER DEFAULT 1,
    path_prefix TEXT,
    is_default INTEGER DEFAULT 0,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_s3_providers_user_id ON s3_providers(user_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_s3_providers_user_id;
DROP TABLE IF EXISTS s3_providers;
-- +goose StatementEnd

