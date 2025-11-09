-- +goose Up
-- +goose StatementBegin
SELECT 'Adding checksum fields to backups table';

-- Add checksum columns for backup integrity verification
ALTER TABLE backups ADD COLUMN md5_hash TEXT;
ALTER TABLE backups ADD COLUMN sha256_hash TEXT;

-- Create index on checksum fields for faster lookups
CREATE INDEX idx_backups_sha256_hash ON backups(sha256_hash);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'Removing checksum fields from backups table';

DROP INDEX IF EXISTS idx_backups_sha256_hash;
ALTER TABLE backups DROP COLUMN md5_hash;
ALTER TABLE backups DROP COLUMN sha256_hash;

-- +goose StatementEnd

