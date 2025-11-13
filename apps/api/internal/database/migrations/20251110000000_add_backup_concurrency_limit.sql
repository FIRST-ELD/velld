-- +goose Up
-- +goose StatementBegin
ALTER TABLE user_settings ADD COLUMN backup_concurrency_limit INTEGER DEFAULT 3;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE user_settings DROP COLUMN backup_concurrency_limit;
-- +goose StatementEnd

