-- +goose Up
-- +goose StatementBegin
ALTER TABLE user_settings ADD COLUMN notify_telegram BOOLEAN DEFAULT FALSE;
ALTER TABLE user_settings ADD COLUMN telegram_bot_token TEXT;
ALTER TABLE user_settings ADD COLUMN telegram_chat_id TEXT;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE user_settings DROP COLUMN notify_telegram;
ALTER TABLE user_settings DROP COLUMN telegram_bot_token;
ALTER TABLE user_settings DROP COLUMN telegram_chat_id;
-- +goose StatementEnd

