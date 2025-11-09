package backup

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/dendianugerah/velld/internal/mail"
	"github.com/dendianugerah/velld/internal/notification"
	"github.com/dendianugerah/velld/internal/settings"
	"github.com/google/uuid"
)

func (s *BackupService) createFailureNotification(connID string, backupErr error) error {

	conn, err := s.connStorage.GetConnection(connID)
	if err != nil {
		log.Printf("Failed to get connection details: %v", err)
		return fmt.Errorf("failed to get connection details: %v", err)
	}

	if conn == nil {
		log.Printf("Connection not found: %s", connID)
		return fmt.Errorf("connection not found: %s", connID)
	}

	if conn.UserID == uuid.Nil {
		log.Printf("Invalid user ID for connection: %s", connID)
		return fmt.Errorf("invalid user ID for connection: %s", connID)
	}

	userSettings, err := s.settingsService.GetUserSettingsInternal(conn.UserID)
	if err != nil {
		log.Printf("Failed to get user settings: %v", err)
		return fmt.Errorf("failed to get user settings: %v", err)
	}

	if userSettings == nil {
		log.Printf("No settings found for user: %s", conn.UserID)
		return fmt.Errorf("no settings found for user: %s", conn.UserID)
	}

	metadata := map[string]interface{}{
		"connection_id": connID,
		"database_name": conn.DatabaseName,
		"database_type": conn.Type,
		"error":         backupErr.Error(),
		"timestamp":     time.Now().Format(time.RFC3339),
	}

	metadataJSON, _ := json.Marshal(metadata)

	// Create dashboard notification if enabled
	if userSettings.NotifyDashboard {
		notification := &notification.Notification{
			ID:        uuid.New(),
			UserID:    conn.UserID,
			Title:     "Backup Failed",
			Message:   fmt.Sprintf("Backup failed for database '%s': %v", conn.DatabaseName, backupErr),
			Type:      notification.BackupFailed,
			Status:    notification.StatusUnread,
			Metadata:  metadataJSON,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		if err := s.notificationRepo.CreateNotification(notification); err != nil {
			fmt.Printf("Error creating dashboard notification: %v\n", err)
		}
	}

	// Send webhook notification if enabled
	if userSettings.NotifyWebhook && userSettings.WebhookURL != nil {
		go s.sendWebhookNotification(*userSettings.WebhookURL, metadata)
	}

	// Send email notification if enabled
	if userSettings.NotifyEmail && userSettings.Email != nil {
		log.Printf("Attempting to send email notification to: %s", *userSettings.Email)
		// Use separate goroutine for email to prevent blocking
		go func(emailAddr string, userSettings *settings.UserSettings, meta map[string]interface{}) {
			if err := s.sendEmailNotification(emailAddr, userSettings, meta); err != nil {
				log.Printf("Failed to send email notification: %v", err)
			}
		}(*userSettings.Email, userSettings, metadata)
	} else {
		log.Printf("Email notification skipped - enabled: %v, email configured: %v",
			userSettings.NotifyEmail, userSettings.Email != nil)
	}

	// Send Telegram notification if enabled
	if userSettings.NotifyTelegram && userSettings.TelegramBotToken != nil && userSettings.TelegramChatID != nil {
		go func(botToken string, chatID string, meta map[string]interface{}) {
			message := formatTelegramMessage(
				"Backup Failed",
				meta["database_name"].(string),
				meta["database_type"].(string),
				"failed",
				meta,
			)
			if err := s.sendTelegramNotification(botToken, chatID, message); err != nil {
				log.Printf("Failed to send Telegram notification: %v", err)
			}
		}(*userSettings.TelegramBotToken, *userSettings.TelegramChatID, metadata)
	}

	return nil
}

// formatBytesForNotification formats bytes to human-readable format
func formatBytesForNotification(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func (s *BackupService) sendWebhookNotification(webhookURL string, data map[string]interface{}) {
	body, _ := json.Marshal(data)
	_, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("Error sending webhook notification: %v\n", err)
	}
}

func (s *BackupService) sendEmailNotification(email string, userSettings *settings.UserSettings, data map[string]interface{}) error {
	if userSettings == nil {
		return fmt.Errorf("settings cannot be nil")
	}

	if userSettings.SMTPHost == nil || userSettings.SMTPUsername == nil ||
		userSettings.SMTPPassword == nil || userSettings.SMTPPort == nil {
		return fmt.Errorf("incomplete SMTP configuration")
	}

	// Check if password is from environment variable (plain text) or database (encrypted)
	password := *userSettings.SMTPPassword

	// If password is configured via env var, it's already plain text
	// Otherwise, it's encrypted in the database and needs to be decrypted
	if userSettings.EnvConfigured == nil || !userSettings.EnvConfigured["smtp_password"] {
		decryptedPassword, err := s.cryptoService.Decrypt(password)
		if err != nil {
			return fmt.Errorf("failed to decrypt SMTP password: %v", err)
		}
		password = decryptedPassword
	}

	smtpConfig := &mail.SMTPConfig{
		Host:     *userSettings.SMTPHost,
		Port:     *userSettings.SMTPPort,
		Username: *userSettings.SMTPUsername,
		Password: password,
	}

	msg := &mail.Message{
		From:    *userSettings.SMTPUsername,
		To:      email,
		Subject: "Velld - Backup Failed",
		Body:    fmt.Sprintf("Backup failed for database '%s'. Error: %v", data["database_name"], data["error"]),
	}

	if err := mail.SendEmail(smtpConfig, msg); err != nil {
		fmt.Printf("Error sending email notification: %v\n", err)
	}

	return nil
}

// createSuccessNotification sends success notifications
func (s *BackupService) createSuccessNotification(connID string, backup *Backup) error {
	conn, err := s.connStorage.GetConnection(connID)
	if err != nil {
		log.Printf("Failed to get connection details: %v", err)
		return fmt.Errorf("failed to get connection details: %v", err)
	}

	if conn == nil {
		log.Printf("Connection not found: %s", connID)
		return fmt.Errorf("connection not found: %s", connID)
	}

	if conn.UserID == uuid.Nil {
		log.Printf("Invalid user ID for connection: %s", connID)
		return fmt.Errorf("invalid user ID for connection: %s", connID)
	}

	userSettings, err := s.settingsService.GetUserSettingsInternal(conn.UserID)
	if err != nil {
		log.Printf("Failed to get user settings: %v", err)
		return fmt.Errorf("failed to get user settings: %v", err)
	}

	if userSettings == nil {
		log.Printf("No settings found for user: %s", conn.UserID)
		return fmt.Errorf("no settings found for user: %s", conn.UserID)
	}

	duration := ""
	if backup.CompletedTime != nil && !backup.StartedTime.IsZero() {
		dur := backup.CompletedTime.Sub(backup.StartedTime)
		duration = fmt.Sprintf("%.0f seconds", dur.Seconds())
	}

	metadata := map[string]interface{}{
		"connection_id": connID,
		"database_name": conn.DatabaseName,
		"database_type": conn.Type,
		"size":          backup.Size,
		"duration":      duration,
		"timestamp":     time.Now().Format(time.RFC3339),
	}

	metadataJSON, _ := json.Marshal(metadata)

	// Create dashboard notification if enabled
	if userSettings.NotifyDashboard {
		notification := &notification.Notification{
			ID:        uuid.New(),
			UserID:    conn.UserID,
			Title:     "Backup Completed",
			Message:   fmt.Sprintf("Backup completed successfully for database '%s'. Size: %s", conn.DatabaseName, formatBytesForNotification(backup.Size)),
			Type:      notification.BackupCompleted,
			Status:    notification.StatusUnread,
			Metadata:  metadataJSON,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		if err := s.notificationRepo.CreateNotification(notification); err != nil {
			fmt.Printf("Error creating dashboard notification: %v\n", err)
		}
	}

	// Send webhook notification if enabled
	if userSettings.NotifyWebhook && userSettings.WebhookURL != nil {
		go s.sendWebhookNotification(*userSettings.WebhookURL, metadata)
	}

	// Send email notification if enabled
	if userSettings.NotifyEmail && userSettings.Email != nil {
		log.Printf("Attempting to send success email notification to: %s", *userSettings.Email)
		go func(emailAddr string, userSettings *settings.UserSettings, meta map[string]interface{}) {
			if err := s.sendSuccessEmailNotification(emailAddr, userSettings, meta); err != nil {
				log.Printf("Failed to send email notification: %v", err)
			}
		}(*userSettings.Email, userSettings, metadata)
	}

	// Send Telegram notification if enabled
	if userSettings.NotifyTelegram && userSettings.TelegramBotToken != nil && userSettings.TelegramChatID != nil {
		go func(botToken string, chatID string, meta map[string]interface{}) {
			message := formatTelegramMessage(
				"Backup Completed",
				meta["database_name"].(string),
				meta["database_type"].(string),
				"success",
				meta,
			)
			if err := s.sendTelegramNotification(botToken, chatID, message); err != nil {
				log.Printf("Failed to send Telegram notification: %v", err)
			}
		}(*userSettings.TelegramBotToken, *userSettings.TelegramChatID, metadata)
	}

	return nil
}

func (s *BackupService) sendSuccessEmailNotification(email string, userSettings *settings.UserSettings, data map[string]interface{}) error {
	if userSettings == nil {
		return fmt.Errorf("settings cannot be nil")
	}

	if userSettings.SMTPHost == nil || userSettings.SMTPUsername == nil ||
		userSettings.SMTPPassword == nil || userSettings.SMTPPort == nil {
		return fmt.Errorf("incomplete SMTP configuration")
	}

	// Check if password is from environment variable (plain text) or database (encrypted)
	password := *userSettings.SMTPPassword

	// If password is configured via env var, it's already plain text
	// Otherwise, it's encrypted in the database and needs to be decrypted
	if userSettings.EnvConfigured == nil || !userSettings.EnvConfigured["smtp_password"] {
		decryptedPassword, err := s.cryptoService.Decrypt(password)
		if err != nil {
			return fmt.Errorf("failed to decrypt SMTP password: %v", err)
		}
		password = decryptedPassword
	}

	smtpConfig := &mail.SMTPConfig{
		Host:     *userSettings.SMTPHost,
		Port:     *userSettings.SMTPPort,
		Username: *userSettings.SMTPUsername,
		Password: password,
	}

	size := ""
	if sizeVal, ok := data["size"].(int64); ok && sizeVal > 0 {
		size = fmt.Sprintf("Size: %s", formatBytesForNotification(sizeVal))
	}

	msg := &mail.Message{
		From:    *userSettings.SMTPUsername,
		To:      email,
		Subject: "Velld - Backup Completed Successfully",
		Body:    fmt.Sprintf("Backup completed successfully for database '%s'. %s", data["database_name"], size),
	}

	if err := mail.SendEmail(smtpConfig, msg); err != nil {
		fmt.Printf("Error sending email notification: %v\n", err)
	}

	return nil
}
