-- +goose Up
-- +goose StatementBegin
ALTER TABLE backups ADD COLUMN logs TEXT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE backups DROP COLUMN logs;
-- +goose StatementEnd

