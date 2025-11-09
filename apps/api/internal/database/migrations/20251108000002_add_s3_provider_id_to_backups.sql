-- +goose Up
-- +goose StatementBegin
ALTER TABLE backups ADD COLUMN s3_provider_id TEXT REFERENCES s3_providers(id) ON DELETE SET NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE backups DROP COLUMN s3_provider_id;
-- +goose StatementEnd

